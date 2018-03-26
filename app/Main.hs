{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable, runProcess)
import qualified Control.Distributed.Process.Node as Local

import Network.Socket (HostName, ServiceName)
import Network.Transport.TCP (createTransport, defaultTCPParameters)
import qualified Network.Transport.TCP.Internal as TCP

import Control.Concurrent
import Control.Monad (forever)
import Options.Applicative
-- import ClassyPrelude
import BasicPrelude
import Data.IORef
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
  optsSendFor <- option auto $ long "send-for" ++ value 2
  optsWaitFor <- option auto $ long "wait-for" ++ value 1
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

type Msg = (Double, Double)
type Block = [Msg]
type BlockChain = [Block]

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
  node <- Local.newLocalNode transport initRemoteTable

  sendOverRef <- newIORef False
  waitOverRef <- newIORef False
  _ <- forkIO $ timeKeeper sendOverRef optsSendFor
  _ <- forkIO $ timeKeeper waitOverRef optsWaitFor
  runProcess node $ process peers rnd sendOverRef waitOverRef
  return ()

timeKeeper :: IORef Bool -> Int -> IO ()
timeKeeper eventRef delay = do
  threadDelay $ delay * 1000000
  atomicWriteIORef eventRef True

process :: [NodeId] -> GenIO -> IORef Bool -> IORef Bool -> Process ()
process peers rnd sendOverRef waitOverRef = do
  thisPid <- getSelfPid
  register "test" thisPid
  inbox <- liftIO $ newIORef ([] :: [Msg])
  let appendMsg msg = liftIO $ modifyIORef inbox $ (++ [msg])
  forever $ do
    liftIO (readIORef sendOverRef) >>= \case
      True -> do
        -- liftIO $ errLn $ "Done: " ++ tshow thisPid
        liftIO $ do
          msgs <- sort <$> readIORef inbox
          let score = sum [fromIntegral i * x | (i, (_, x)) <- zip [1 :: Int ..] msgs]
          printf "%.0f\n" score
        terminate
      False -> do
        outMsg <- liftIO $ do
          val <- liftIO $ getRandom rnd
          time :: Double <- realToFrac <$> getPOSIXTime
          return (time, val)
        forM_ peers $ \peer -> nsendRemote peer "test" outMsg
        appendMsg outMsg
        m <- expectTimeout 1000000
        case m of
          Nothing  -> say "Nothing..."
          Just (msg :: Msg) -> do
            say $ "Received " ++ show msg
            appendMsg msg

hostPortToNode :: (String, String) -> NodeId
hostPortToNode (host, port) = NodeId $ TCP.encodeEndPointAddress host port 0

initRandomGen :: Integral a => a -> IO GenIO
initRandomGen seed = Rnd.initialize $ V.singleton (fromIntegral seed)

getRandom :: Rnd.GenIO -> IO Double
getRandom = Rnd.uniformR (0.0, 1.0)
