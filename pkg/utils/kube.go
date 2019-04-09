package utils

import (
	"fmt"
	"log"

	"github.com/maorfr/skbn/pkg/skbn"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetPods returns a slice of strings of pod names by namespace and selector
func GetPods(iClient interface{}, namespace, selector string) ([]string,[]string, error) {

	k8sClient := *iClient.(*skbn.K8sClient)
	pods, err := k8sClient.ClientSet.CoreV1().Pods(namespace).List(metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, nil, err
	}

	var podIPList []string
	var podList []string
	for _, pod := range pods.Items {
		podList = append(podList, pod.Name)
		podIPList = append(podIPList, pod.Status.PodIP)
	}
	if len(podList) == 0 {
		return nil, nil, fmt.Errorf("No pods were found in namespace %s by selector %s", namespace, selector)
	}


	log.Printf("PodIP: %v", podIPList)

	return podIPList, podList, nil
}
