{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Main where

import Control.Distributed.Process hiding (newChan)
import qualified Control.Distributed.Process as DP
import Control.Distributed.Process.Node

import Network.Socket (HostName, ServiceName)
import Network.Transport.TCP (createTransport, defaultTCPParameters)
import qualified Network.Transport.TCP.Internal as TCP

import Control.Concurrent
import Control.Exception (throw)
import Control.Monad (forever)
import Control.Concurrent.MVar
import Control.Concurrent.STM.TChan
import Control.Concurrent.STM
import Control.Error.Util

import Options.Applicative
import BasicPrelude
import qualified Data.Map as M
import Data.IORef
import System.Random.MWC
import Data.Binary
import Data.Vector.Binary ()
import GHC.Generics
import System.IO
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Vector as V
import qualified System.Random.MWC as Rnd
import Text.Printf (printf)
import qualified Data.ByteString.Char8 as B8
import Data.Time

data Opts = Opts
  { optsNodeIx :: Int
  , optsSendFor :: Int
  , optsWaitFor :: Int
  , optsSeed :: Int
  , optsSendMax :: Maybe Int
  } deriving (Show)

type PeerList = [(String, String)]
type HostPort = (String, String)

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
  , envSendTimeOver :: IO Bool
  , envRecvTimeOver :: IO Bool
  , envStartTime :: UTCTime
  , envScoreMVar :: MVar (Int, Double)
  }

getNodeList :: IO [((String, String), NodeId)]
getNodeList = do
  rows <- T.lines <$> T.readFile "nodes.txt"
  return $ map node rows
  where
  node (map T.unpack . T.splitOn ":" -> [host, port]) =
    ((host, port), NodeId $ TCP.encodeEndPointAddress host port 0)
  node line = terror $ "Node address must be in form host:port. Got this instead: " ++ line

main :: IO ()
main = do
  envStartTime <- getCurrentTime
  opts@Opts{..} <- execParser $ info (helper <*> optsParser) $ progDesc ""
  nodes <- getNodeList
  let nNodes = length nodes
  when (optsNodeIx >= nNodes) $
    error $ "node-index too high. Must be from 0 to " ++ show (nNodes - 1)
  let envNodeIds = map snd nodes
  let ((host, port), thisNode) = nodes !! optsNodeIx

  transport <- either throw id <$> createTransport host port (host,) defaultTCPParameters
  localNode <- newLocalNode transport initRemoteTable

  let getTimeDelta = do
        now <- getCurrentTime
        return (now, diffUTCTime now envStartTime)

  let envSay :: forall m. MonadIO m => Text -> m ()
      envSay text = liftIO $ do
        (now, delta) <- getTimeDelta
        let prefix1 = T.pack $ formatTime defaultTimeLocale "%T%3Q " now
        let prefix2 = T.pack $ printf "%.3f" $ (realToFrac delta :: Double)
        let line = T.unwords [prefix1, prefix2, text]
        B8.hPutStrLn stderr $ encodeUtf8 line

  envState <- newMVar (NodeState mempty (V.replicate nNodes 0))
  rnd <- initRandomGen (optsSeed + optsNodeIx)
  let envSendTimeOver = do
        (now, delta) <- getTimeDelta
        return $ delta >= fromIntegral optsSendFor
  let envRecvTimeOver = (>= fromIntegral (optsSendFor + optsWaitFor)) . snd <$> getTimeDelta
  envScoreMVar <- newEmptyMVar
  let env = NodeEnv{envOpts = opts, envRandom = getRandom rnd, ..}

  let otherNodes = [(i, node) | (i, node) <- zip [0..] envNodeIds, i /= optsNodeIx]
  senders <- forM otherNodes $ \(otherIx, otherNodeId) -> do
    forkProcess localNode $ msgSender env otherIx otherNodeId
  forM_ otherNodes $ \(otherIx, otherNodeId) -> do
    forkProcess localNode $ msgReceiver env otherIx otherNodeId
  _ <- forkProcess localNode $ generator env senders
  envSay $ "Started "  ++ tshow thisNode
  takeMVar envScoreMVar >> computeScore env
  takeMVar envScoreMVar >>= \score -> do
    let out = T.pack $ uncurry (printf "(%d, %.0f)") score
    T.putStrLn out
    envSay $ "===> Score: " ++ out

generator :: NodeEnv -> [ProcessId] -> Process ()
generator NodeEnv{..} senderPids = do
  thisPid <- getSelfPid
  register "generator" thisPid
  loop 0
  where
  Opts{..} = envOpts
  loop nSent = do
    _ :: () <- expect
    over <- liftIO envSendTimeOver
    if | over || maybe False (nSent >=) optsSendMax -> do
        envSay "Generator done"
        liftIO $ putMVar envScoreMVar (0, 0)
        terminate
       | otherwise -> do
        msgValue <- liftIO envRandom
        (msg, size) <- liftIO $ modifyMVar envState $ \state -> do
          let msgTime = incrementTime optsNodeIx (stateTime state)
          let msg = Msg { msgNodeIx = optsNodeIx, .. }
          let state' = state {stateTime = msgTime, stateHist = M.insert msgTime msgValue (stateHist state)}
          return (state', (msg, M.size (stateHist state')))
        envSay $ T.intercalate "\t" ["size: " ++ tshow size, "generator", tshowMsg msg]
        forM_ senderPids $ \pid -> send pid msg
        loop (nSent + 1)

msgSender :: NodeEnv -> Int -> NodeId -> Process ()
msgSender env@NodeEnv{..} recvIx recvNodeId = do
  thisPid <- getSelfPid
  register thisName thisPid
  recvPid <- getRemotePid env thisName recvNodeId recvName
  loop recvPid 0
  where
  Opts{..} = envOpts
  thisName = regName "msgSender" recvIx
  recvName = regName "msgReceiver" optsNodeIx
  loop recvPid nSent = do
    -- instantly try to pick message from inbox, and if failed, request a new message from generator
    msg :: Msg <- expectTimeout 0 >>= maybe (nsend "generator" () >> expect) return
    send recvPid msg
    envSay $ T.intercalate "\t" [T.pack thisName, tshowMsg msg]
    loop recvPid (nSent + 1)

msgReceiver :: NodeEnv -> Int -> NodeId -> Process ()
msgReceiver NodeEnv{..} senderIx senderNodeId = do
  thisPid <- getSelfPid
  register thisName thisPid
  forever $ do
    liftIO envRecvTimeOver >>= \case
      True -> do
        envSay $ "Done: " ++ T.pack thisName
        terminate
      False -> do
        msg@Msg{..} <- expect
        (time, size) <- liftIO $ modifyMVar envState $ \state -> do
          let time' = mergeTime optsNodeIx msgTime (stateTime state)
          let state' = state {stateTime = time', stateHist = M.insert msgTime msgValue (stateHist state)}
          let size = M.size (stateHist state')
          return (state', (time', size))
        envSay $ T.intercalate "\t" ["size: " ++ tshow size, T.pack thisName, tshowMsg msg, "@" ++ tshow time]
  where
  Opts{..} = envOpts
  thisName = regName "msgReceiver" senderIx

computeScore :: NodeEnv -> IO ()
computeScore NodeEnv{..} = do
  threadDelay $ (optsWaitFor envOpts * 10^6 - 500*10^3 :: Int)
  hist <- stateHist <$> takeMVar envState
  let score = sum [fromIntegral i * val | (i, (_, val)) <- zip [1 :: Int ..] (M.toAscList hist)]
  putMVar envScoreMVar (M.size hist, score)

getRemotePid :: NodeEnv -> String -> NodeId -> String -> Process ProcessId
getRemotePid NodeEnv{..} thisName otherNodeId otherName = loop 0
  where
  loop n = do
    liftIO envRecvTimeOver >>= \case
      True -> do
        envSay "getRemotePid done"
        terminate
      False -> do
        thisPid <- getSelfPid
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

tshowMsg :: Msg -> Text
tshowMsg Msg{..} = T.pack $ printf "%2d %.3f %s" msgNodeIx msgValue (show msgTime)

regName :: String -> Int -> String
regName name i = name ++ show i

initRandomGen :: Integral a => a -> IO GenIO
initRandomGen seed = Rnd.initialize $ V.singleton (fromIntegral seed)

getRandom :: Rnd.GenIO -> IO Double
getRandom = Rnd.uniformR (0.0, 1.0)

incrementTime :: Int -> Timestamp -> Timestamp
incrementTime thisIx = V.imap (\i -> if i == thisIx then succ else id)

mergeTime :: Int -> Timestamp -> Timestamp -> Timestamp
mergeTime thisIx senderTime thisTime =
  V.zipWith max senderTime thisTime

optsParser :: Parser Opts
optsParser = do
  optsNodeIx <- option auto $ long "node-index" ++ short 'i'
  optsSendFor <- option auto $ short 's' ++ long "send-for" ++ value 2
  optsSendMax <- optional $ option auto $ long "send-max"
  optsWaitFor <- option auto $ short 'w' ++ long "wait-for" ++ value 1
  optsSeed <- option auto $ long "with-seed" ++ value 0
  return Opts{..}