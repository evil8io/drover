package projectsync

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"slices"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	// rancherCluster is the cluster id of the Rancher cluster itself. It holds
	// the Project objects of every cluster.
	rancherCluster = "local"

	projectsWatchPath = "/k8s/clusters/" + rancherCluster + "/apis/management.cattle.io/v3/projects"
)

// projectObject is the part of a Project object of management.cattle.io that
// the service reads.
type projectObject struct {
	Metadata struct {
		Name              string            `json:"name"`
		Namespace         string            `json:"namespace"`
		ResourceVersion   string            `json:"resourceVersion"`
		CreationTimestamp string            `json:"creationTimestamp"`
		Labels            map[string]string `json:"labels"`
		Annotations       map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		ClusterName string `json:"clusterName"`
		DisplayName string `json:"displayName"`
	} `json:"spec"`
}

// watchedProject is the pruned project of one project watch event. namespace
// and name are the metadata of the Project object: the cluster id and the
// project name.
type watchedProject struct {
	namespace       string
	name            string
	resourceVersion string
	project         project
}

// decodeProject decodes the Project object of an event into the fields that
// the project list of Norman gives, and prunes it at once.
func (s *Syncer) decodeProject(raw json.RawMessage) (watchedProject, error) {
	var object projectObject
	if err := json.Unmarshal(raw, &object); err != nil {
		return watchedProject{}, err
	}
	return watchedProject{
		namespace:       object.Metadata.Namespace,
		name:            object.Metadata.Name,
		resourceVersion: object.Metadata.ResourceVersion,
		project: s.pruneProject(project{
			ID:          object.Metadata.Namespace + ":" + object.Metadata.Name,
			ClusterID:   object.Spec.ClusterName,
			Name:        object.Spec.DisplayName,
			CreatorID:   object.Metadata.Annotations[creatorAnnotation],
			Created:     object.Metadata.CreationTimestamp,
			Labels:      object.Metadata.Labels,
			Annotations: object.Metadata.Annotations,
		}),
	}, nil
}

// watchProjects keeps the project watch of the Rancher cluster open.
func (s *Syncer) watchProjects(ctx context.Context, watches *watchSet) {
	s.keepWatching(ctx, kindProject, rancherCluster, func(ctx context.Context, resourceVersion string) (string, error) {
		return s.streamProjects(ctx, resourceVersion, watches)
	})
}

// streamProjects reads one project watch stream until it ends. It passes the
// project of every event but BOOKMARK to applyProject.
func (s *Syncer) streamProjects(ctx context.Context, resourceVersion string, watches *watchSet) (string, error) {
	return s.readWatch(ctx, kindProject, rancherCluster, projectsWatchPath, "", resourceVersion,
		func(event watchEvent) (string, error) {
			item, err := s.decodeProject(event.Object)
			if err != nil {
				return "", err
			}
			if event.Type != watchBookmark {
				s.applyProject(ctx, watches, event.Type, item)
			}
			return item.resourceVersion, nil
		})
}

// applyProject brings one project event into the snapshot. A project of a
// cluster outside the snapshot waits for the next reconcile run, and it logs
// no warning, because the service does not manage that cluster. A change puts
// the project into the queue of its cluster, and a new display name clears
// the name cache of the worker first.
func (s *Syncer) applyProject(ctx context.Context, watches *watchSet, eventType string, item watchedProject) {
	cluster, id := item.namespace, item.project.ID
	projects := s.projectsOf(cluster)
	if projects == nil {
		s.logger.DebugContext(ctx, "project skipped, the cluster is not in the snapshot",
			"cluster", cluster, "project", id)
		return
	}
	if cluster != item.project.ClusterID {
		s.logger.WarnContext(ctx, "the project namespace is not its cluster name",
			"project", id, "cluster_name", item.project.ClusterID)
		return
	}
	// The project label of a namespace can hold only a label value.
	if len(item.name) > maxLabelValueLength || !qualifiedName.MatchString(item.name) {
		s.logger.DebugContext(ctx, "project skipped, the name is not a label value", "project", id)
		return
	}

	switch eventType {
	case watchDeleted:
		s.deleteProject(cluster, item.name)
		s.logger.DebugContext(ctx, "project deleted", "cluster", cluster, "project", id)
		return
	case watchAdded, watchModified:
	default:
		return
	}

	old, known := projects[item.name]
	if known && old.Name == item.project.Name &&
		old.CreatorID == item.project.CreatorID &&
		old.reserved == item.project.reserved && old.marked == item.project.marked &&
		maps.Equal(old.Labels, item.project.Labels) &&
		maps.Equal(old.Annotations, item.project.Annotations) {
		s.logger.DebugContext(ctx, "project unchanged", "cluster", cluster, "project", id)
		return
	}
	if !s.setProject(cluster, item.name, item.project) {
		s.logger.DebugContext(ctx, "project skipped, the cluster left the snapshot",
			"cluster", cluster, "project", id)
		return
	}
	nameChanged := old.Name != item.project.Name
	s.metrics.projectChanged(ctx, cluster)
	s.logger.InfoContext(ctx, "project changed",
		"cluster", cluster, "project", id,
		"labels", changedKeys(old.Labels, item.project.Labels),
		"annotations", changedKeys(old.Annotations, item.project.Annotations),
		"name_changed", nameChanged)

	watch := watches.get(cluster)
	if watch == nil {
		s.logger.DebugContext(ctx, "project not queued, the cluster has no watch yet",
			"cluster", cluster, "project", id)
		return
	}
	if nameChanged {
		watch.resetNames()
	}
	watch.projects.put(item.name)
}

// changedKeys returns the keys that one map has and the other has not, or
// has with another value, sorted.
func changedKeys(before, after map[string]string) []string {
	var keys []string
	for key, value := range after {
		if have, ok := before[key]; !ok || have != value {
			keys = append(keys, key)
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// projectLister lists the namespaces of the projects of one cluster queue, one
// project at a time. The limiter of the cluster bounds the lists together
// with the patches of the worker.
func (s *Syncer) projectLister(ctx context.Context, cluster string, watch *clusterWatch) {
	for {
		name, ok := watch.projects.next(ctx)
		if !ok {
			return
		}
		if watch.limiter.wait(ctx) {
			s.listProject(ctx, cluster, name, watch.patches)
		}
		watch.projects.done()
		if ctx.Err() != nil {
			return
		}
	}
}

// listProject puts every namespace of a project into patches. A list that
// fails is a warning, and the next reconcile run repeats the work.
func (s *Syncer) listProject(ctx context.Context, cluster, name string, patches *queue[patchItem]) {
	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace list request failed", err,
			"cluster", cluster, "project", name)
		return
	}
	if s.serviceAccounts {
		s.ensureProjectAccounts(ctx, token, cluster, name)
	}
	items, err := s.namespaces(ctx, token, cluster, projectLabel+"="+name)
	if err != nil {
		s.logListFailure(ctx, err, cluster, "project", name)
		return
	}
	for _, item := range items {
		patches.put(patchItem{target: item, origin: originProject})
	}
}
