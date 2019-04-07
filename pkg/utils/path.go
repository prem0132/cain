package utils

import (
	"bytes"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/maorfr/skbn/pkg/skbn"
)

// GetFromToPathsCassandraKeySpace gets files to copy for keyspace
func GetFromToPathsCassandraKeySpace(iClient interface{}, pods []string, namespace, container, keyspace, tag, dstBasePath, cassandraDataDir string) ([]skbn.FromToPair, error) {
	k8sClient := iClient.(*skbn.K8sClient)
	var fromToPathsAllPods []skbn.FromToPair
	for _, pod := range pods {

		fromToPaths, err := GetFromAndToPathsKeySpaceK8sToDst(k8sClient, namespace, pod, container, keyspace, tag, dstBasePath, cassandraDataDir)
		if err != nil {
			return nil, err
		}
		fromToPathsAllPods = append(fromToPathsAllPods, fromToPaths...)
	}
	return fromToPathsAllPods, nil
}

// GetFromAndToPathsFromK8s aggregates paths from all pods
func GetFromAndToPathsFromK8s(iClient interface{}, pods []string, namespace, container, keyspace, tag, dstBasePath, cassandraDataDir string) ([]skbn.FromToPair, error) {
	k8sClient := iClient.(*skbn.K8sClient)
	var fromToPathsAllPods []skbn.FromToPair
	for _, pod := range pods {

		fromToPaths, err := GetFromAndToPathsK8sToDst(k8sClient, namespace, pod, container, keyspace, tag, dstBasePath, cassandraDataDir)
		if err != nil {
			return nil, err
		}
		fromToPathsAllPods = append(fromToPathsAllPods, fromToPaths...)
	}

	return fromToPathsAllPods, nil
}

// GetFromAndToPathsSrcToK8s performs a path mapping between a source and Kubernetes
func GetFromAndToPathsSrcToK8s(srcClient, k8sClient interface{}, srcPrefix, srcPath, srcBasePath, namespace, container, cassandraDataDir string) ([]skbn.FromToPair, []string, []string, error) {
	var fromToPaths []skbn.FromToPair

	filesToCopyRelativePaths, err := skbn.GetListOfFiles(srcClient, srcPrefix, srcPath)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(filesToCopyRelativePaths) == 0 {
		return nil, nil, nil, fmt.Errorf("No files found to restore")
	}

	pods := make(map[string]string)
	tables := make(map[string]string)
	testedPaths := make(map[string]string)
	for _, fileToCopyRelativePath := range filesToCopyRelativePaths {

		fromPath := filepath.Join(srcPath, fileToCopyRelativePath)
		toPath, err := PathFromSrcToK8s(k8sClient, fromPath, cassandraDataDir, srcBasePath, namespace, container, pods, tables, testedPaths)
		if err != nil {
			return nil, nil, nil, err
		}

		fromToPaths = append(fromToPaths, skbn.FromToPair{FromPath: fromPath, ToPath: toPath})
	}

	return fromToPaths, MapKeysToSlice(pods), MapKeysToSlice(tables), nil
}

//REDUNDANT FROM CQLSH
// Cqlsh executes cqlsh -e 'command' in a given pod
func Cqlsh(iK8sClient interface{}, namespace, pod, container string, command []string) ([]byte, error) {
	k8sClient := iK8sClient.(*skbn.K8sClient)

	command = append([]string{"cqlsh", "-e"}, command...)
	stdout := new(bytes.Buffer)
	stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil, stdout)

	if len(stderr) != 0 {
		return nil, fmt.Errorf("STDERR: " + (string)(stderr))
	}
	if err != nil {
		return nil, err
	}

	return removeWarning(stdout.Bytes()), nil
}

//GETTING TABLES
// GetTables gets the list of tables
func GetTables(iK8sClient interface{}, namespace, pod, container string) ([]byte, error) {
	command := []string{fmt.Sprintf("DESCRIBE TABLES;")}
	output, err := Cqlsh(iK8sClient, namespace, pod, container, command)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Tables:")
	log.Println((string)(output))
	log.Println("\n\n\n\nEOF")
	return output, nil
}

//REDUNDANT FROM CQLSH
func removeWarning(b []byte) []byte {
	const warning = "Warning: Cannot create directory at `/home/cassandra/.cassandra`. Command history will not be saved."
	return []byte(strings.Replace((string)(b), warning, "", 1))
}

// GetFromAndToPathsKeySpaceK8sToDst performs a path mapping between Kubernetes and a destination
func GetFromAndToPathsKeySpaceK8sToDst(k8sClient interface{}, namespace, pod, container, keyspace, tag, dstBasePath, cassandraDataDir string) ([]skbn.FromToPair, error) {
	var fromToPaths []skbn.FromToPair
	log.Println(namespace, pod, container, keyspace, tag, dstBasePath, cassandraDataDir)

	pathPrfx := filepath.Join(namespace, pod, container, cassandraDataDir)
	log.Println("PathPrefix:", pathPrfx)
	keyspacePath := filepath.Join(pathPrfx, keyspace)
	log.Println("Filepath for Keyspace: ", keyspacePath)
	GetTables(k8sClient, namespace, pod, container)
	tablesRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, keyspacePath, "d", "tweet*")
	// tablesRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, keyspacePath, "f", "*")
	// tablesRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, keyspacePath, "f", "*")
	log.Println("tablesRelativePaths", tablesRelativePaths)
	if err != nil {
		return nil, err
	}

	for _, tableRelativePath := range tablesRelativePaths {
		log.Println("tableRelativePath", tableRelativePath)
		tablePath := filepath.Join(keyspacePath, tableRelativePath)
		log.Println("tablePath", tablePath)
		filesToCopyRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, tablePath, "f", "*")
		if err != nil {
			return nil, err
		}
		log.Println("filesToCopyRelativePaths", filesToCopyRelativePaths)
		var newfilesToCopyRelativePaths []string
		for _, checkfilesToCopyRelativePaths := range filesToCopyRelativePaths {
			log.Println("checkfilesToCopyRelativePaths", checkfilesToCopyRelativePaths)
			if strings.Contains(checkfilesToCopyRelativePaths, "/backup") || strings.Contains(checkfilesToCopyRelativePaths, "/snapshots") {
				log.Println("Skipping...")
			} else {
				newfilesToCopyRelativePaths = append(newfilesToCopyRelativePaths, checkfilesToCopyRelativePaths)
			}
		}

		//for _, fileToCopyRelativePath := range newfilesToCopyRelativePaths {
		for _, fileToCopyRelativePath := range newfilesToCopyRelativePaths {

			log.Println("fileToCopyRelativePath", fileToCopyRelativePath)
			fromPath := filepath.Join(tablePath, fileToCopyRelativePath)
			log.Println("PathFromK8sToDst Arguments: \n", fromPath, cassandraDataDir, dstBasePath)
			toPath := PathFromK8sToDstKeySpace(fromPath, cassandraDataDir, dstBasePath)

			fromToPaths = append(fromToPaths, skbn.FromToPair{FromPath: fromPath, ToPath: toPath})
		}
	}
	log.Println("fromToPaths", fromToPaths)
	return fromToPaths, nil

}

// PathFromK8sToDstKeySpace maps a single path from Kubernetes to destination
func PathFromK8sToDstKeySpace(k8sPath, cassandraDataDir, dstBasePath string) string {
	k8sPath = strings.Replace(k8sPath, cassandraDataDir, "", 1)
	log.Println("k8spath:", k8sPath)
	pSplit := strings.Split(k8sPath, "/")

	// 0 = namespace
	pod := pSplit[1]
	// 2 = container
	// 3 = keyspace
	keyspace := pSplit[3]
	tableWithHash := pSplit[4]
	// 5 = snapshots
	//tag := pSplit[6]
	file := ""
	if len(k8sPath) < 6 {
		file = pSplit[5]
	} else {
		file = pSplit[5] + "/" + pSplit[6]
	}

	table := strings.Split(tableWithHash, "-")[0]
	log.Println("destPath: ", filepath.Join(dstBasePath, pod, keyspace, table, file))
	return filepath.Join(dstBasePath, pod, keyspace, table, file)
}

// PathFromK8sToDst maps a single path from Kubernetes to destination
func PathFromK8sToDst(k8sPath, cassandraDataDir, dstBasePath string) string {
	k8sPath = strings.Replace(k8sPath, cassandraDataDir, "", 1)
	log.Println("k8spath:", k8sPath)
	pSplit := strings.Split(k8sPath, "/")

	// 0 = namespace
	pod := pSplit[1]
	// 2 = container
	// 3 = keyspace
	tableWithHash := pSplit[4]
	// 5 = snapshots
	tag := pSplit[6]
	file := pSplit[7]

	table := strings.Split(tableWithHash, "-")[0]

	return filepath.Join(dstBasePath, tag, pod, table, file)
}

// GetFromAndToPathsK8sToDst performs a path mapping between Kubernetes and a destination
func GetFromAndToPathsK8sToDst(k8sClient interface{}, namespace, pod, container, keyspace, tag, dstBasePath, cassandraDataDir string) ([]skbn.FromToPair, error) {
	var fromToPaths []skbn.FromToPair
	pathPrfx := filepath.Join(namespace, pod, container, cassandraDataDir)
	log.Println("pathprefix:", pathPrfx)
	keyspacePath := filepath.Join(pathPrfx, keyspace)
	tablesRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, keyspacePath, "d", tag)
	log.Println("table and key path:\n", keyspacePath, tablesRelativePaths)
	if err != nil {
		return nil, err
	}

	for _, tableRelativePath := range tablesRelativePaths {

		tablePath := filepath.Join(keyspacePath, tableRelativePath)
		filesToCopyRelativePaths, err := skbn.GetListOfFilesFromK8s(k8sClient, tablePath, "f", "*")
		if err != nil {
			return nil, err
		}

		for _, fileToCopyRelativePath := range filesToCopyRelativePaths {
			log.Println("fileToCopyRelativePath", fileToCopyRelativePath)
			fromPath := filepath.Join(tablePath, fileToCopyRelativePath)
			log.Println("PathFromK8sToDst Arguments: \n", fromPath, cassandraDataDir, dstBasePath)
			toPath := PathFromK8sToDst(fromPath, cassandraDataDir, dstBasePath)

			fromToPaths = append(fromToPaths, skbn.FromToPair{FromPath: fromPath, ToPath: toPath})
		}
	}
	log.Println("fromToPaths", fromToPaths)
	return fromToPaths, nil
}

// PathFromSrcToK8s maps a single path from source to Kubernetes
func PathFromSrcToK8s(k8sClient interface{}, fromPath, cassandraDataDir, srcBasePath, namespace, container string, pods, tables, testedPaths map[string]string) (string, error) {
	fromPath = strings.Replace(fromPath, srcBasePath+"/", "", 1)
	pSplit := strings.Split(fromPath, "/")

	keyspace := pSplit[0]
	// 1 = sum
	// 2 = tag
	pod := pSplit[3]
	table := pSplit[4]
	file := pSplit[5]

	pods[pod] = "hello there!"
	tables[table] = "hello there!"

	k8sKeyspacePath := filepath.Join(namespace, pod, container, cassandraDataDir, keyspace)

	// Don`t test the same path twice
	pathToTest := filepath.Join(k8sKeyspacePath, table)
	if tablePath, ok := testedPaths[pathToTest]; ok {
		toPath := filepath.Join(tablePath, file)
		return toPath, nil
	}

	tableRelativePath, err := skbn.GetListOfFilesFromK8s(k8sClient, k8sKeyspacePath, "d", table+"-*")
	if err != nil {
		return "", err
	}
	if len(tableRelativePath) != 1 {
		return "", fmt.Errorf("Error with table %s, found %d directories", table, len(tableRelativePath))
	}

	tablePath := filepath.Join(k8sKeyspacePath, tableRelativePath[0])
	testedPaths[pathToTest] = tablePath
	toPath := filepath.Join(tablePath, file)

	return toPath, nil
}

// ChangeFilesOwnership changes the ownership of files after restoring them
func ChangeFilesOwnership(iK8sClient interface{}, pods []string, namespace, container, userGroup, cassandraDataDir string) error {
	k8sClient := iK8sClient.(*skbn.K8sClient)
	command := []string{"chown", "-R", userGroup, cassandraDataDir}
	for _, pod := range pods {
		stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil, nil)
		if len(stderr) != 0 {
			return fmt.Errorf("STDERR: " + (string)(stderr))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// TestK8sDirectory checks if a path exists
func TestK8sDirectory(iK8sClient interface{}, pods []string, namespace, container, cassandraDataDir string) error {
	k8sClient := iK8sClient.(*skbn.K8sClient)
	command := []string{"ls", cassandraDataDir}
	for _, pod := range pods {
		stderr, err := skbn.Exec(*k8sClient, namespace, pod, container, command, nil, nil)
		if len(stderr) != 0 {
			return fmt.Errorf("STDERR: " + (string)(stderr))
		}
		if err != nil {
			return fmt.Errorf(cassandraDataDir + " does not exist. " + err.Error())
		}
	}
	return nil
}
