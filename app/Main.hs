{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE MultiWayIf #-}
module Main where

import Control.Distributed.Process hiding (newChan)
import qualified Control.Distributed.Process as DP
import Control.Distributed.Process.Node

import Network.Socket (HostName, ServiceName)
import Network.Transport.TCP (createTransport, defaultTCPParameters)
import qualified Network.Transport.TCP.Internal as TCP

import Control.Concurrent
import Control.Monad (forever)
import Options.Applicative
-- import ClassyPrelude
import BasicPrelude
import qualified Data.Map as M
import Data.IORef
import Control.Concurrent.MVar
import Control.Error.Util
import System.Random.MWC
import Data.Binary
import Data.Vector.Binary ()
import GHC.Generics

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified System.Random.MWC as Rnd
import Text.Printf (printf)
import Data.Time.Clock.POSIX
import Data.Time.Format

data Opts = Opts
  -- { optsHost :: Maybe String
  -- , optsPort :: Maybe String
  { optsPeerIx :: Int
  , optsSendFor :: Int
  , optsWaitFor :: Int
  , optsSeed :: Int
  , optsSendMax :: Maybe Int
  } deriving (Show)

type PeerList = [(String, String)]
type HostPort = (String, String)

peersConf :: PeerList
peersConf =
  [ ("127.0.0.1", "12300")
  , ("127.0.0.1", "12301")
  , ("127.0.0.1", "12302")
  ]

data Msg = Msg
  { msgNodeIx :: Int
  , msgValue :: Double
  , msgTime :: Timestamp
  } deriving (Show, Generic)
instance Binary Msg

type History = Map Timestamp Double
type Timestamp = V.Vector Int

data NodeState = NodeState
  { stateHist :: !History
  , stateTime :: !Timestamp
  } deriving (Show)

data NodeEnv = NodeEnv
  { envOpts :: Opts
  , envNodeIds :: [NodeId]
  , envSay :: forall m. MonadIO m => Text -> m ()
  , envState :: MVar NodeState
  , envRandom :: IO Double
  , envSendTimeOver :: IORef Bool
  , envRecvTimeOver :: IORef Bool
  }

main :: IO ()
main = do
  opts@Opts{..} <- execParser $ info (helper <*> optsParser) $ progDesc ""
  let hostPort@(host, port) = peersConf !! optsPeerIx
  -- print (host, port)

  let thisNode = hostPortToNode hostPort
  -- let otherPeers = filter (/= thisNode ) $ map hostPortToNode peersConf
  let envNodeIds = map hostPortToNode peersConf
  let nPeers = length envNodeIds

  Right transport <- createTransport host port (\port' -> (host, port')) defaultTCPParameters
  localNode <- newLocalNode transport initRemoteTable

  stderrChan <- newChan
  loggerDone <- newEmptyMVar
  let writeLog text = do
          t <- T.pack . formatTime defaultTimeLocale "%T%3Q " <$> getCurrentTime
          errLn (t ++ text)

  let loop = do
        s <- readChan stderrChan
        if s == mempty then putMVar loggerDone "Logger done" else writeLog s >> loop
  logger <- forkIO $ loop
  let envSay :: forall m. MonadIO m => Text -> m ()
      envSay = liftIO . writeChan stderrChan
  envState <- newMVar (NodeState mempty (V.replicate nPeers 0))
  rnd <- initRandomGen optsSeed
  envSendTimeOver <- newIORef False
  envRecvTimeOver <- newIORef False
  let env = NodeEnv{envOpts = opts, envRandom = getRandom rnd, ..}

  scoreVar <- newEmptyMVar
  _ <- forkIO $ timer envSendTimeOver optsSendFor >> computeScore env optsWaitFor (putMVar scoreVar)
  _ <- forkIO $ timer envRecvTimeOver (optsSendFor + optsWaitFor)
  senders <- forM (zip [0..] envNodeIds) $ \(otherIx, otherNodeId) -> do
    forkProcess localNode $ msgSender env otherIx otherNodeId
  forM_ (zip [0..] envNodeIds) $ \(otherIx, otherNodeId) -> do
    forkProcess localNode $ msgReceiver env otherIx otherNodeId
  _ <- forkProcess localNode $ msgGenerator env senders
  writeLog $ "Started "  ++ tshow thisNode
  takeMVar scoreVar >>= uncurry (printf "%d\t%3.0f\n")
  envSay ""
  takeMVar loggerDone >>= writeLog

msgGenerator :: NodeEnv -> [ProcessId] -> Process ()
msgGenerator NodeEnv{..} senderPids = loop 0
  where
  Opts{..} = envOpts
  loop nSent = do
    over <- liftIO (readIORef envSendTimeOver)
    if | over || maybe False (nSent >=) optsSendMax -> do
        envSay "Generator done"
        terminate
       | otherwise -> do
        val <- liftIO envRandom
        msg <- liftIO $ modifyMVar envState $ \state@NodeState{..} -> do
          let time' = incrementTime optsPeerIx stateTime
          let msg = Msg { msgNodeIx = optsPeerIx, msgTime = time', msgValue = val }
          return (state, msg)
        forM_ senderPids $ \pid -> send pid msg
        loop (nSent + 1)

-- msgSender :: Int -> [NodeId] -> IO Double -> IORef Bool -> MVar NodeState -> Process ()
msgSender :: NodeEnv -> Int -> NodeId -> Process ()
msgSender NodeEnv{..} recvIx recvNodeId = do
  thisPid <- getSelfPid
  register thisName thisPid
  loop 0
  where
  Opts{..} = envOpts
  thisName = regName "msgSender" recvIx
  recvName = regName "msgReceiver" optsPeerIx
  loop nSent = do
    msg :: Msg <- expect
    nsendRemote recvNodeId recvName msg
    envSay $ T.unwords [T.pack thisName, tshow msg]
    loop (nSent + 1)

-- msgReceiver :: Int -> [NodeId] -> IORef Bool -> MVar NodeState -> Process ()
msgReceiver :: NodeEnv -> Int -> NodeId -> Process ()
msgReceiver NodeEnv{..} senderIx senderNodeIx = do
  thisPid <- getSelfPid
  register thisName thisPid
  forever $ do
    liftIO (readIORef envRecvTimeOver) >>= \case
      True -> do
        envSay $ "Done: " ++ T.pack thisName
        terminate
      False -> do
        msg@Msg{..} <- expect
        envSay $ T.unwords $ [T.pack thisName, tshow msg]
        liftIO $ modifyMVar envState $ \state@NodeState{..} -> do
          let time' = mergeTime optsPeerIx msgTime stateTime
          return (state { stateTime = time', stateHist = M.insert time' msgValue stateHist }, ())
  where
  Opts{..} = envOpts
  thisName = regName "msgReceiver" senderIx

computeScore :: NodeEnv -> Int -> ((Int, Double) -> IO ()) -> IO ()
computeScore NodeEnv{..} waitFor putScore = do
  threadDelay $ (waitFor * 900 * 10^3 :: Int)
  msgs <- M.toAscList . stateHist <$> takeMVar envState
  let score = sum [fromIntegral i * val | (i, (_, val)) <- zip [1 :: Int ..] msgs]
  putScore (length msgs, score)

timer :: IORef Bool -> Int -> IO ()
timer eventRef delay = do
  threadDelay $ delay * 10^6
  atomicWriteIORef eventRef True

regName :: String -> Int -> String
regName name i = name ++ " " ++ show i

hostPortToNode :: (String, String) -> NodeId
hostPortToNode (host, port) = NodeId $ TCP.encodeEndPointAddress host port 0

initRandomGen :: Integral a => a -> IO GenIO
initRandomGen seed = Rnd.initialize $ V.singleton (fromIntegral seed)

getRandom :: Rnd.GenIO -> IO Double
getRandom = Rnd.uniformR (0.0, 1.0)

incrementTime :: Int -> Timestamp -> Timestamp
incrementTime thisIx = V.imap (\i -> if i == thisIx then succ else id)

mergeTime :: Int -> Timestamp -> Timestamp -> Timestamp
mergeTime thisIx senderTime thisTime =
  V.izipWith (\i that this -> if i == thisIx then this + 1 else max that this) senderTime thisTime

optsParser :: Parser Opts
optsParser = do
  -- optsHost <- optional $ strOption $ long "host" ++ short 'H'
  -- optsPort <- optional $ strOption $ long "port" ++ short 'P'
  optsPeerIx <- option auto $ long "peer-ix" ++ short 'i'
  optsSendFor <- option auto $ short 's' ++ long "send-for" ++ value 2
  optsSendMax <- optional $ option auto $ long "send-max"
  optsWaitFor <- option auto $ short 'w' ++ long "wait-for" ++ value 1
  optsSeed <- option auto $ long "with-seed" ++ value 0
  return Opts{..}
