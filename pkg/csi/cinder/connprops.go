/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cinder

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// ConnectorPropertiesAnnotation is the node annotation key where the
	// node plugin stores its os-brick connector properties (JSON).  The
	// controller reads this annotation to pass connector properties to
	// Cinder's Attachment API (AttachmentCreate).
	ConnectorPropertiesAnnotation = "cinder.csi.openstack.org/connector-properties"
)

// ConnectorPropertiesGetter retrieves host connector properties
// (iSCSI initiator, FC WWPNs, etc.) for a given CSI node ID.
type ConnectorPropertiesGetter interface {
	GetConnectorProperties(ctx context.Context, nodeID string) (map[string]any, error)
}

// kubeConnectorPropertiesGetter reads connector properties from Kubernetes
// Node annotations.  It uses CSINode objects to map the CSI NodeId to a
// Kubernetes Node name, then reads the annotation from that Node.
type kubeConnectorPropertiesGetter struct {
	csiNodeLister storagev1listers.CSINodeLister
	nodeLister    corev1listers.NodeLister
}

// NewKubeConnectorPropertiesGetter creates a ConnectorPropertiesGetter
// backed by Kubernetes Node annotations.  It starts informers for Node
// and CSINode objects and blocks until the caches are synced.
func NewKubeConnectorPropertiesGetter(kubeClient kubernetes.Interface) ConnectorPropertiesGetter {
	factory := informers.NewSharedInformerFactory(kubeClient, 0)
	ctx := context.TODO()

	nodeInformer := factory.Core().V1().Nodes().Informer()
	csiNodeInformer := factory.Storage().V1().CSINodes().Informer()

	go nodeInformer.Run(ctx.Done())
	go csiNodeInformer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced, csiNodeInformer.HasSynced) {
		klog.Fatal("Error syncing Node/CSINode informer caches for connector properties")
	}

	klog.Info("Successfully created connector properties getter with Node and CSINode listers")

	return &kubeConnectorPropertiesGetter{
		csiNodeLister: factory.Storage().V1().CSINodes().Lister(),
		nodeLister:    factory.Core().V1().Nodes().Lister(),
	}
}

func (g *kubeConnectorPropertiesGetter) GetConnectorProperties(ctx context.Context, nodeID string) (map[string]any, error) {
	// Find the Kubernetes Node name by searching CSINode objects for the
	// one where the cinder.csi.openstack.org driver has the matching nodeID.
	csiNodes, err := g.csiNodeLister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list CSINode objects: %w", err)
	}

	var k8sNodeName string
	for _, csiNode := range csiNodes {
		for _, driver := range csiNode.Spec.Drivers {
			if driver.Name == driverName && driver.NodeID == nodeID {
				k8sNodeName = csiNode.Name
				break
			}
		}
		if k8sNodeName != "" {
			break
		}
	}

	if k8sNodeName == "" {
		return nil, fmt.Errorf("no CSINode found with driver %s and nodeID %s", driverName, nodeID)
	}

	// Read the connector properties annotation from the Kubernetes Node.
	node, err := g.nodeLister.Get(k8sNodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get node %s: %w", k8sNodeName, err)
	}

	propsJSON, ok := node.Annotations[ConnectorPropertiesAnnotation]
	if !ok {
		return nil, fmt.Errorf("connector properties annotation %s not found on node %s", ConnectorPropertiesAnnotation, k8sNodeName)
	}

	var props map[string]any
	if err := json.Unmarshal([]byte(propsJSON), &props); err != nil {
		return nil, fmt.Errorf("failed to parse connector properties from node %s: %w", k8sNodeName, err)
	}

	klog.V(4).Infof("Retrieved connector properties for nodeID %s (node %s)", nodeID, k8sNodeName)
	return props, nil
}
