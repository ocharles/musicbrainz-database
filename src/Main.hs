{-# LANGUAGE OverloadedStrings #-}
import Control.Exception
import Control.Monad (void, when)
import Control.Proxy
import Control.Proxy.ByteString (hGetS)
import Control.Proxy.Parse (wrap)
import Control.Proxy.PostgreSQL.Simple
import Control.Proxy.Tar
import Control.Proxy.Trans.Either
import Control.Proxy.Trans.State
import Data.ByteString (ByteString)
import Data.Foldable (for_, forM_)
import Data.List (stripPrefix)
import Data.String (fromString)
import System.IO

import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import Data.Text.Encoding

import qualified Database.PostgreSQL.Simple as Pg
import qualified Database.PostgreSQL.Simple.Internal as Pg
import qualified Database.PostgreSQL.LibPQ as LibPQ


--------------------------------------------------------------------------------
-- | Given a 'Pg.Connection', attempt to stream @mbdump/@ prefixed 'TarEntry's
-- into the database using @COPY@.
importTables :: Proxy p => Pg.Connection -> TarEntry ->
    Consumer (TarP p) (Maybe ByteString) IO ()
importTables c e =
    for_ (stripPrefix "mbdump/" (entryPath e)) $ \tableName -> do
        let t = if tableName == "editor_sanitised" then "editor" else tableName
        lift (putStrLn $ "Importing " ++ t)
        (wrap . tarEntry e >-> liftP . copyTextIn c t) ()


--------------------------------------------------------------------------------
data Options = Options { createUser :: Bool
                       , createDb :: Bool
                       , createSchemas :: Bool
                       , createTables :: Bool
                       , createIndexes :: Bool
                       , createPrimaryKeys :: Bool
                       , createForeignKeys :: Bool
                       , createConstraints :: Bool
                       , createExtensions :: Bool
                       , importArchives :: [FilePath]
                       , createFunctions :: Bool
                       , setSequences :: Bool
                       } deriving (Show)


--------------------------------------------------------------------------------
main :: IO ()
main = execParser opts >>= run

    where execParser = const $ return Options
            { createUser = False
            , createDb = True
            , createSchemas = True
            , createTables = True
            , createIndexes = True
            , createPrimaryKeys = True
            , createForeignKeys = True
            , createConstraints = True
            , createExtensions = True
            , createFunctions = True
            , setSequences = True
            --, importArchives = []
            , importArchives = map ("/mnt/fedora-postgres/" ++)
                [ "mbdump-cdstubs.tar", "mbdump-cover-art-archive.tar", "mbdump-derived.tar", "mbdump-documentation.tar", "mbdump-editor.tar", "mbdump-edit.tar", "mbdump-stats.tar", "mbdump.tar", "mbdump-wikidocs.tar" ]
            }
          opts = True


rootCon = Pg.defaultConnectInfo { Pg.connectUser = "root", Pg.connectDatabase = "template1" }
mbUserCon = rootCon { Pg.connectUser = "musicbrainz" }
mbCon = mbUserCon { Pg.connectDatabase = "musicbrainz" }
mbRoot = rootCon { Pg.connectDatabase = "musicbrainz" }

--------------------------------------------------------------------------------
run :: Options -> IO ()
run opt = do
    when (createUser opt || createDb opt) $ do
        pg <- Pg.connect rootCon
        when (createUser opt) $ void $ do
            putStrLn "Creating user"
            Pg.execute_ pg "CREATE USER musicbrainz"
            Pg.execute_ pg "ALTER USER musicbrainz SET search_path = musicbrainz, public"

        when (createDb opt) $ void $ do
            putStrLn "Creating database"
            Pg.execute_ pg "CREATE DATABASE musicbrainz WITH OWNER musicbrainz"

    pg <- Pg.connect mbCon

    when (createSchemas opt) $ void $ do
        putStrLn "Creating schemas"
        mapM_ (Pg.execute_ pg . fromString . ("CREATE SCHEMA " ++))
          [ "musicbrainz", "cover_art_archive", "documentation", "report", "statistics", "wikidocs" ]

    when (createExtensions opt) $ void $ do
        putStrLn "Creating extensions"
        pg <- Pg.connect mbRoot
        runPsql pg "sql/Extensions.sql"

    when (createTables opt) $ void $
        mapM_ (runPsql pg . (++ "/CreateTables.sql"))
          [ "sql", "sql/caa", "sql/documentation", "sql/report", "sql/statistics", "sql/wikidocs" ]

    when (createFunctions opt) $ void $ do
        putStrLn "Creating functions"
        resetAll pg
        mapM_ (runPsql pg . (++ "/CreateFunctions.sql")) [ "sql", "sql/caa" ]

    forM_ (importArchives opt) $ \f -> withFile f ReadMode $ \dump -> void $ do
        putStrLn $ "Importing " ++ f
        resetAll pg
        runProxy $ runEitherK $ evalStateK mempty $
            wrap . hGetS (32 * 1024 * 1024) dump >-> tarArchive />/ importTables pg

    when (createIndexes opt) $ void $ do
        putStrLn "Creating Indexes"
        mapM_ (runPsql pg . (++ "/CreateIndexes.sql")) [ "sql", "sql/caa","sql/statistics" ]

    when (createPrimaryKeys opt) $ void $ do
        putStrLn "Creating primary keys"
        mapM_ (runPsql pg . (++ "/CreatePrimaryKeys.sql"))
            [ "sql", "sql/caa", "sql/documentation", "sql/statistics", "sql/wikidocs" ]

    when (createForeignKeys opt) $ void $ do
        putStrLn "Creating Foreign Keys"
        mapM_ (runPsql pg . (++ "/CreateFKConstraints.sql")) [ "sql", "sql/caa" ]

    when (createConstraints opt) $ void $ do
        putStrLn "Creating constraints"
        runPsql pg "sql/CreateConstraints.sql"

    when (setSequences opt) $ void $ do
        putStrLn "Resetting sequences"
        mapM_ (runPsql pg . (++ "/SetSequences.sql")) [ "sql", "sql/statistics" ]

--------------------------------------------------------------------------------
runPsql :: Pg.Connection -> FilePath -> IO ()
runPsql pg f = do
    resetAll pg
    Pg.withConnection pg $ \pq -> void $
        Text.readFile f >>= LibPQ.exec pq . stripPsql
  where
    stripPsql = encodeUtf8 . Text.unlines . filter (not . ("\\" `Text.isPrefixOf`)) .
                  Text.lines

resetAll pg = Pg.execute_ pg "RESET ALL"
