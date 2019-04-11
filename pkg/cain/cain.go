package cain

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/maorfr/skbn/pkg/skbn"
	"github.com/prem0132/cain/pkg/utils"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// AddDataOptions are the options to add data
type AddDataOptions struct {
	Loop      bool
	Executors int
	Run       int
	Namespace string
	Selector  string
	Keyspace  string
	Table     string
}

// AddData add dummy data to the cassandra Cluster
func AddData(o AddDataOptions) (string, error) {
	log.Printf("function for adding data!!!")
	log.Printf("options:%v ", o)

	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return "", err
	}
	pods, podsIP, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return "", err
	}

	log.Printf("Pods Detected: %v", pods)
	log.Printf("ip of cassandra: %v", podsIP)

	cluster := gocql.NewCluster(podsIP[0])
	cluster.Keyspace = o.Keyspace
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	var tableName string
	if err := session.Query(`SELECT table_name
        FROM system_schema.tables WHERE keyspace_name= ? and table_name= ? LIMIT 1`,
		o.Keyspace, o.Table).Consistency(gocql.One).Scan(&tableName); err != nil {
		log.Fatal(err)
	}

	log.Printf("Table Name: %v", tableName)

	if o.Run > 1 {
		o.Executors = 32
	} else if o.Loop == true {
		o.Executors = 32
	}
	runtime.GOMAXPROCS(o.Executors)
	log.Printf("MaxProcsSetat")

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// insert a tweet
		if o.Loop == true {
			i := 0
			for {
				log.Printf("Data: %v		%v 		%v", time.Now().Format("20060102150405"), gocql.TimeUUID(), i)
				i = i + 1
				if err := session.Query(`INSERT INTO tweet (timeline, id, text, subtext) VALUES ( ? , ? , ? , ? )`,
					time.Now().Format("20060102150405"), i, gocql.TimeUUID(), gocql.TimeUUID()).Exec(); err != nil {
					log.Fatal(err)
				}
			}
		} else {
			for r := 0; r < o.Run; r++ {
				log.Printf("Data: %v		%v 		%v", time.Now().Format("20060102150405"), gocql.TimeUUID(), r)
				if err := session.Query(`INSERT INTO tweet (timeline, id, text, subtext) VALUES ( ? , ? , ? , ? )`,
					time.Now().Format("20060102150405"), r, gocql.TimeUUID(), gocql.TimeUUID()).Exec(); err != nil {
					log.Fatal(err)
				}
			}
		}
	}()

	return "", nil
}

type CqlshOptions struct {
	Namespace string
	Command   string
	Selector  string
}

func CqlshExec(o CqlshOptions) (string, error) {

	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return "", err
	}
	pods, _, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return "", err
	}

	newCommand := []string{fmt.Sprintf("%v", o.Command)}
	output, err := Cqlsh(k8sClient, o.Namespace, pods[0], "", newCommand)

	log.Printf("Command exited with:%v", string(output))
	return "", nil
}

type NodeToolOptions struct {
	Namespace string
	Command   string
	Selector  string
}

func NodeTool(o NodeToolOptions) (string, error) {

	pSplit := strings.Split(o.Command, " ")
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return "", err
	}
	pods, _, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return "", err
	}

	output, err := Newnodetool(k8sClient, o.Namespace, pods[0], "", pSplit)

	log.Printf("Command exited with: \n %v", output)
	return "", nil
}

// BackupOptions are the options to pass to Backup
type BackupOptions struct {
	Namespace        string
	Selector         string
	Container        string
	Keyspace         string
	Dst              string
	Parallel         int
	BufferSize       float64
	CassandraDataDir string
}

// Backup performs backup
func Backup(o BackupOptions) (string, error) {
	log.Println("Backup started!")
	dstPrefix, dstPath := utils.SplitInTwo(o.Dst, "://")

	if err := skbn.TestImplementationsExist("k8s", dstPrefix); err != nil {
		return "", err
	}

	log.Println("Getting clients")
	k8sClient, dstClient, err := skbn.GetClients("k8s", dstPrefix, "", dstPath)
	if err != nil {
		return "", err
	}

	log.Println("Getting pods")
	pods, _, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return "", err
	}

	log.Println("Testing existence of data dir")
	if err := utils.TestK8sDirectory(k8sClient, pods, o.Namespace, o.Container, o.CassandraDataDir); err != nil {
		return "", err
	}

	log.Println("Backing up schema")
	dstBasePath, err := BackupKeyspaceSchema(k8sClient, dstClient, o.Namespace, pods[0], o.Container, o.Keyspace, dstPrefix, dstPath)
	if err != nil {
		return "", err
	}

	log.Println("Taking snapshots")
	tag := TakeSnapshots(k8sClient, pods, o.Namespace, o.Container, o.Keyspace)

	log.Println("Calculating paths. This may take a while...")
	fromToPathsAllPods, err := utils.GetFromAndToPathsFromK8s(k8sClient, pods, o.Namespace, o.Container, o.Keyspace, tag, dstBasePath, o.CassandraDataDir)
	if err != nil {
		return "", err
	}

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(k8sClient, dstClient, "k8s", dstPrefix, fromToPathsAllPods, o.Parallel, o.BufferSize); err != nil {
		return "", err
	}

	log.Println("Clearing snapshots")
	ClearSnapshots(k8sClient, pods, o.Namespace, o.Container, o.Keyspace, tag)

	log.Println("All done!")
	return tag, nil
}

// RestoreOptions are the options to pass to Restore
type RestoreOptions struct {
	Src              string
	Keyspace         string
	Tag              string
	Schema           string
	Namespace        string
	Selector         string
	Container        string
	Parallel         int
	BufferSize       float64
	UserGroup        string
	CassandraDataDir string
}

// Restore performs restore
func Restore(o RestoreOptions) error {
	log.Println("Restore started!")
	srcPrefix, srcBasePath := utils.SplitInTwo(o.Src, "://")

	log.Println("Getting clients")
	srcClient, k8sClient, err := skbn.GetClients(srcPrefix, "k8s", srcBasePath, "")
	if err != nil {
		return err
	}

	log.Println("Getting pods")
	existingPods, _, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return err
	}

	log.Println("Testing existence of data dir")
	if err := utils.TestK8sDirectory(k8sClient, existingPods, o.Namespace, o.Container, o.CassandraDataDir); err != nil {
		return err
	}

	log.Println("Getting current schema")
	_, sum, err := DescribeKeyspaceSchema(k8sClient, o.Namespace, existingPods[0], o.Container, o.Keyspace)
	if err != nil {
		if o.Schema == "" {
			return err
		}
		log.Println("Schema not found, restoring schema", o.Schema)
		sum, err = RestoreKeyspaceSchema(srcClient, k8sClient, srcPrefix, srcBasePath, o.Namespace, existingPods[0], o.Container, o.Keyspace, o.Schema, o.Parallel, o.BufferSize)
		if err != nil {
			return err
		}
		log.Println("Restored schema:", sum)
	}

	if o.Schema != "" && sum != o.Schema {
		return fmt.Errorf("specified schema %s is not the same as found schema %s", o.Schema, sum)
	}

	log.Println("Found schema:", sum)

	log.Println("Calculating paths. This may take a while...")
	srcPath := filepath.Join(srcBasePath, o.Keyspace, sum, o.Tag)
	fromToPaths, podsToBeRestored, tablesToRefresh, err := utils.GetFromAndToPathsSrcToK8s(srcClient, k8sClient, srcPrefix, srcPath, srcBasePath, o.Namespace, o.Container, o.CassandraDataDir)
	if err != nil {
		return err
	}

	log.Println("Validating pods match restore")
	if err := utils.SliceContainsSlice(podsToBeRestored, existingPods); err != nil {
		return err
	}

	log.Println("Getting materialized views to exclude")
	materializedViews, err := GetMaterializedViews(k8sClient, o.Namespace, o.Container, existingPods[0], o.Keyspace)
	if err != nil {
		return err
	}

	log.Println("Truncating tables")
	TruncateTables(k8sClient, o.Namespace, o.Container, o.Keyspace, existingPods, tablesToRefresh, materializedViews)

	log.Println("Starting files copy")
	if err := skbn.PerformCopy(srcClient, k8sClient, srcPrefix, "k8s", fromToPaths, o.Parallel, o.BufferSize); err != nil {
		return err
	}

	log.Println("Changing files ownership")
	if err := utils.ChangeFilesOwnership(k8sClient, existingPods, o.Namespace, o.Container, o.UserGroup, o.CassandraDataDir); err != nil {
		return err
	}

	log.Println("Refreshing tables")
	RefreshTables(k8sClient, o.Namespace, o.Container, o.Keyspace, podsToBeRestored, tablesToRefresh)

	log.Println("All done!")
	return nil
}

// SchemaOptions are the options to pass to Schema
type SchemaOptions struct {
	Namespace string
	Selector  string
	Container string
	Keyspace  string
}

// Schema gets the schema of the cassandra cluster
func Schema(o SchemaOptions) ([]byte, string, error) {
	k8sClient, err := skbn.GetClientToK8s()
	if err != nil {
		return nil, "", err
	}
	pods, _, err := utils.GetPods(k8sClient, o.Namespace, o.Selector)
	if err != nil {
		return nil, "", err
	}
	schema, sum, err := DescribeKeyspaceSchema(k8sClient, o.Namespace, pods[0], o.Container, o.Keyspace)
	if err != nil {
		return nil, "", err
	}

	return schema, sum, nil
}
