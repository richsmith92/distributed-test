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
import qualified Data.Text.IO as T
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
  , ("127.0.0.1", "12303")
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
  rnd <- initRandomGen (optsSeed + optsPeerIx)
  envSendTimeOver <- newIORef False
  envRecvTimeOver <- newIORef False
  let env = NodeEnv{envOpts = opts, envRandom = getRandom rnd, ..}

  scoreVar <- newEmptyMVar
  _ <- forkIO $ timer envSendTimeOver optsSendFor >> computeScore env optsWaitFor (putMVar scoreVar)
  _ <- forkIO $ timer envRecvTimeOver (optsSendFor + optsWaitFor)
  let otherNodes = [(i, node) | (i, node) <- zip [0..] envNodeIds, i /= optsPeerIx]
  senders <- forM otherNodes $ \(otherIx, otherNodeId) -> do
    forkProcess localNode $ msgSender env otherIx otherNodeId
  forM_ otherNodes $ \(otherIx, otherNodeId) -> do
    forkProcess localNode $ msgReceiver env otherIx otherNodeId
  _ <- forkProcess localNode $ generator env senders
  writeLog $ "Started "  ++ tshow thisNode
  takeMVar scoreVar >>= \score -> do
    let out = T.pack $ uncurry (printf "(%d, %.0f)") score
    T.putStrLn out
    envSay $ "===> Score: " ++ out
    envSay ""
    takeMVar loggerDone >>= writeLog

generator :: NodeEnv -> [ProcessId] -> Process ()
generator NodeEnv{..} senderPids = do
  thisPid <- getSelfPid
  register "generator" thisPid
  loop 0
  where
  Opts{..} = envOpts
  loop nSent = do
    _ :: () <- expect
    over <- liftIO (readIORef envSendTimeOver)
    if | over || maybe False (nSent >=) optsSendMax -> do
        envSay "Generator done"
        terminate
       | otherwise -> do
        msgValue <- liftIO envRandom
        (msg, size) <- liftIO $ modifyMVar envState $ \state -> do
          let msgTime = incrementTime optsPeerIx (stateTime state)
          let msg = Msg { msgNodeIx = optsPeerIx, .. }
          let state' = state {stateTime = msgTime, stateHist = M.insert msgTime msgValue (stateHist state)}
          return (state', (msg, M.size (stateHist state')))
        envSay $ T.intercalate "\t" ["History size: " ++ tshow size, "generator", tshowMsg msg]
        forM_ senderPids $ \pid -> send pid msg
        loop (nSent + 1)

-- msgSender :: Int -> [NodeId] -> IO Double -> IORef Bool -> MVar NodeState -> Process ()
msgSender :: NodeEnv -> Int -> NodeId -> Process ()
msgSender env@NodeEnv{..} recvIx recvNodeId = do
  thisPid <- getSelfPid
  register thisName thisPid
  recvPid <- getRemotePid env thisName recvNodeId recvName
  loop recvPid 0
  where
  Opts{..} = envOpts
  thisName = regName "msgSender" recvIx
  recvName = regName "msgReceiver" optsPeerIx
  loop recvPid nSent = do
    -- instantly try to pick message from inbox, and if failed, request a new message from generator
    msg :: Msg <- expectTimeout 0 >>= maybe (nsend "generator" () >> expect) return
    send recvPid msg
    envSay $ T.intercalate "\t" [T.pack thisName, tshowMsg msg]
    loop recvPid (nSent + 1)

-- msgReceiver :: Int -> [NodeId] -> IORef Bool -> MVar NodeState -> Process ()
msgReceiver :: NodeEnv -> Int -> NodeId -> Process ()
msgReceiver NodeEnv{..} senderIx senderNodeId = do
  thisPid <- getSelfPid
  register thisName thisPid
  -- envSay $ T.intercalate "\t" [T.pack thisName, "sent pid"]
  -- nsendRemote senderNodeId senderName thisPid
  -- senderPid :: ProcessId <- expect
  -- envSay $ T.intercalate "\t" [T.pack thisName, "received pid"]
  forever $ do
    liftIO (readIORef envRecvTimeOver) >>= \case
      True -> do
        envSay $ "Done: " ++ T.pack thisName
        terminate
      False -> do
        msg@Msg{..} <- expect
        (time, size) <- liftIO $ modifyMVar envState $ \state -> do
          let time' = mergeTime optsPeerIx msgTime (stateTime state)
          let state' = state {stateTime = time', stateHist = M.insert msgTime msgValue (stateHist state)}
          let size = M.size (stateHist state')
          return (state', (time', size))
        envSay $ T.intercalate "\t" ["History size: " ++ tshow size, T.pack thisName, tshowMsg msg, "@" ++ tshow time]
        -- envSay $ T.unwords ["Inserted", tshowMsg msg]
  where
  Opts{..} = envOpts
  -- senderName = regName "msgSender" optsPeerIx
  thisName = regName "msgReceiver" senderIx

computeScore :: NodeEnv -> Int -> ((Int, Double) -> IO ()) -> IO ()
computeScore NodeEnv{..} waitFor putScore = do
  threadDelay $ (waitFor * 900 * 10^3 :: Int)
  hist <- stateHist <$> takeMVar envState
  let score = sum [fromIntegral i * val | (i, (_, val)) <- zip [1 :: Int ..] (M.toAscList hist)]
  putScore (M.size hist, score)

getRemotePid :: NodeEnv -> String -> NodeId -> String -> Process ProcessId
getRemotePid NodeEnv{..} thisName otherNodeId otherName = loop 0
  where
  loop n = do
    thisPid <- getSelfPid
    -- envSay $ T.intercalate "\t" [T.pack thisName, "sent pid"]
    whereisRemoteAsync otherNodeId otherName
    let timeoutMicro = min 50000 (round $ 1.1 ^ n)
    expectTimeout timeoutMicro >>= \case
      Just (WhereIsReply name (Just pid)) -> do -- yay!
        envSay $ T.unwords [T.pack thisName, "connected"]
        return pid
      other -> do
        case other of
          Just (WhereIsReply wtf _) ->
            envSay $ T.intercalate "\t" ["WARN", T.pack thisName, "whereis: ", T.pack wtf]
          Nothing -> return ()
        loop (n + 1)

timer :: IORef Bool -> Int -> IO ()
timer eventRef delay = do
  threadDelay $ delay * 10^6
  atomicWriteIORef eventRef True

tshowMsg :: Msg -> Text
tshowMsg Msg{..} = T.pack $ printf "( %2d %.3f %s )" msgNodeIx msgValue (show msgTime)

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
  -- V.izipWith (\i that this -> if i == thisIx then this + 1 else max that this) senderTime thisTime
  V.zipWith max senderTime thisTime

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
