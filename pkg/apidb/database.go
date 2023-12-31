package apidb

import (
	"database/sql"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/node-crawler/pkg/crawlerdb"
	"github.com/ethereum/node-crawler/pkg/vparser"
)

func CreateDB(db *sql.DB) error {
	sqlStmt := `
		CREATE TABLE nodes (
			id                  TEXT NOT NULL,
			name                TEXT,
			version_major       NUMBER,
			version_minor       NUMBER,
			version_patch       NUMBER,
			version_tag         TEXT,
			version_build       TEXT,
			version_date        TEXT,
			os_name             TEXT,
			os_architecture     TEXT,
			language_name       TEXT,
			language_version    TEXT,
			last_crawled        DATETIME,
			country_name        TEXT,
			networkid           TEXT,
			forkid              TEXT,

			PRIMARY KEY (ID)
		);

		DELETE FROM nodes;
	`
	_, err := db.Exec(sqlStmt)
	return err
}

func InsertCrawledNodes(db *sql.DB, crawledNodes []crawlerdb.CrawledNode) error {
	log.Info("Writing nodes to db", "len", len(crawledNodes))

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO nodes(
			id,
			name,
			version_major,
			version_minor,
			version_patch,
			version_tag,
			version_build,
			version_date,
			os_name,
			os_architecture,
			language_name,
			language_version,
			last_crawled,
			country_name,
			networkid,
			forkid
		)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE
		SET
			name = excluded.name,
			version_major = excluded.version_major,
			version_minor = excluded.version_minor,
			version_patch = excluded.version_patch,
			version_tag = excluded.version_tag,
			version_build = excluded.version_build,
			version_date = excluded.version_date,
			os_name = excluded.os_name,
			os_architecture = excluded.os_architecture,
			language_name = excluded.language_name,
			language_version = excluded.language_version,
			last_crawled = excluded.last_crawled,
			country_name = excluded.country_name,
			networkid = excluded.networkid,
			forkid = excluded.forkid
		WHERE
			name = excluded.name
			OR excluded.name != "unknown"
	`)
	if err != nil {
		return err
	}

	// It's possible for us to have the same node scraped multiple times, so
	// we want to make sure when we are upserting, we get the most recent
	// scrape upserted last.
	sort.SliceStable(crawledNodes, func(i, j int) bool {
		return strings.Compare(crawledNodes[i].Now, crawledNodes[j].Now) < 0
	})

	for _, node := range crawledNodes {
		parsed := vparser.ParseVersionString(node.ClientType)
		if parsed != nil {
			_, err = stmt.Exec(
				node.ID,
				parsed.Name,
				parsed.Version.Major,
				parsed.Version.Minor,
				parsed.Version.Patch,
				parsed.Version.Tag,
				parsed.Version.Build,
				parsed.Version.Date,
				parsed.Os.Os,
				parsed.Os.Architecture,
				parsed.Language.Name,
				parsed.Language.Version,
				time.Now(),
				node.Country,
				node.NetworkID,
				node.ForkID,
			)
			if err != nil {
				panic(err)
			}
		}
	}
	return tx.Commit()
}

func DropOldNodes(db *sql.DB, minTimePassed time.Duration) error {
	log.Info("Dropping nodes", "older than", minTimePassed)
	oldest := time.Now().Add(-minTimePassed)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`DELETE FROM nodes WHERE last_crawled < ?`)
	if err != nil {
		return err
	}
	res, err := stmt.Exec(oldest)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	log.Info("Nodes drop", "affected", affected)
	return tx.Commit()
}
