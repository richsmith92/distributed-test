{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Distributed.Process
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

import qualified Data.Vector as V
import qualified System.Random.MWC as Rnd
import Text.Printf (printf)
import Data.Time.Clock.POSIX

data Opts = Opts
  { optsHost :: Maybe String
  , optsPort :: Maybe String
  , optsPeerId :: Maybe String
  , optsSendFor :: Int
  , optsWaitFor :: Int
  , optsSeed :: Int
  } deriving (Show)

optsParser :: Parser Opts
optsParser = do
  optsHost <- optional $ strOption $ long "host" ++ short 'H'
  optsPort <- optional $ strOption $ long "port" ++ short 'P'
  optsPeerId <- optional $ strOption $ long "peer-id" ++ short 'i'
  optsSendFor <- option auto $ short 's' ++ long "send-for" ++ value 2
  optsWaitFor <- option auto $ short 'w' ++ long "wait-for" ++ value 1
  optsSeed <- option auto $ long "with-seed" ++ value 0
  return Opts{..}

type PeerList = [(String, (String, String))]
type HostPort = (String, String)

peersConf :: PeerList
peersConf =
  [ ("0", ("127.0.0.1", "12300"))
  , ("1", ("127.0.0.1", "12301"))
  , ("2", ("127.0.0.1", "12302"))
  ]

blockSize :: Int
blockSize = 1000

type Msg = Double
type History = Map Timestamp Double
type Timestamp = Int

data NodeState = NodeState
  { stateHist :: !History
  , stateTime :: !Timestamp
  } deriving (Show)

main :: IO ()
main = do
  opts@Opts{..} <- execParser $ info (helper <*> optsParser) $ progDesc ""
  let Just hostPort@(host, port) =
        ((,) <$> optsHost <*> optsPort) <|> (optsPeerId >>= \i -> lookup i peersConf)
  -- print (host, port)

  let thisNode = hostPortToNode hostPort
  let peers = filter (/= thisNode ) $ map (hostPortToNode .  snd) peersConf
  rnd <- initRandomGen optsSeed

  Right transport <- createTransport host port (\port' -> (host, port')) defaultTCPParameters
  node <- newLocalNode transport initRemoteTable

  sendOverRef <- newIORef False
  waitOverRef <- newIORef False
  stateVar <- newMVar (NodeState mempty 0)
  scoreVar <- newEmptyMVar
  _ <- forkIO $ timer sendOverRef optsSendFor >> computeScore optsWaitFor stateVar (putMVar scoreVar)
  _ <- forkIO $ timer waitOverRef (optsSendFor + optsWaitFor)
  _ <- forkProcess node $ msgSender peers rnd sendOverRef stateVar
  _ <- forkProcess node $ msgReceiver peers waitOverRef stateVar
  errLn $ "Started "  ++ tshow thisNode
  takeMVar scoreVar >>= uncurry (printf "%d\t%3.0f\n")

timer :: IORef Bool -> Int -> IO ()
timer eventRef delay = do
  threadDelay $ delay * 10^6
  atomicWriteIORef eventRef True

computeScore :: Int -> MVar NodeState -> ((Int, Double) -> IO ()) -> IO ()
computeScore waitFor stateVar putScore = do
  threadDelay $ (waitFor * 900 * 10^3 :: Int)
  msgs <- M.toAscList . stateHist <$> takeMVar stateVar
  let score = sum [fromIntegral i * val | (i, (_, val)) <- zip [1 :: Int ..] msgs]
  putScore (length msgs, score)

msgSender :: [NodeId] -> GenIO -> IORef Bool -> MVar NodeState -> Process ()
msgSender peers rnd sendOverRef stateVar = do
  thisPid <- getSelfPid
  register "msgSender" thisPid
  forever $ do
    liftIO (readIORef sendOverRef) >>= \case
      True -> do
        terminate
      False -> do
        msg <- liftIO $ modifyMVar stateVar $ \state@NodeState{..} -> do
          val <- getRandom rnd
          let time' = stateTime + 1
          let msg = (time', val)
          return (state { stateTime = time', stateHist = M.insert time' val stateHist }, msg)
        forM_ peers $ \peer -> nsendRemote peer "msgReceiver" msg
        say $ "Sent " ++ show msg

msgReceiver :: [NodeId] -> IORef Bool -> MVar NodeState -> Process ()
msgReceiver _peers waitOverRef stateVar = do
  thisPid <- getSelfPid
  register "msgReceiver" thisPid
  forever $ do
    liftIO (readIORef waitOverRef) >>= \case
      True -> do
        terminate
      False -> do
        msg@(time, val) <- expect
        say $ "Received " ++ show msg
        liftIO $ modifyMVar stateVar $ \state@NodeState{..} -> do
          let time' = max time stateTime
          return (state { stateTime = time', stateHist = M.insert time' val stateHist }, ())


hostPortToNode :: (String, String) -> NodeId
hostPortToNode (host, port) = NodeId $ TCP.encodeEndPointAddress host port 0

initRandomGen :: Integral a => a -> IO GenIO
initRandomGen seed = Rnd.initialize $ V.singleton (fromIntegral seed)

getRandom :: Rnd.GenIO -> IO Double
getRandom = Rnd.uniformR (0.0, 1.0)
