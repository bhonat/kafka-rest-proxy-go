package franz

import "github.com/twmb/franz-go/pkg/kgo"

const unspecifiedPartition int32 = -1

// explicitPartitioner preserves Confluent REST Proxy's optional per-record
// partition field while falling back to franz-go's high-throughput default
// partitioner when the client does not specify a partition.
type explicitPartitioner struct {
	fallback kgo.Partitioner
}

func newExplicitPartitioner() kgo.Partitioner {
	return explicitPartitioner{
		fallback: kgo.UniformBytesPartitioner(64*1024, true, true, nil),
	}
}

func (p explicitPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	return explicitTopicPartitioner{fallback: p.fallback.ForTopic(topic)}
}

type explicitTopicPartitioner struct {
	fallback kgo.TopicPartitioner
}

func (p explicitTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	if r.Partition >= 0 {
		return true
	}
	return p.fallback.RequiresConsistency(r)
}

func (p explicitTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if r.Partition >= 0 {
		return int(r.Partition)
	}
	return p.fallback.Partition(r, n)
}

func (p explicitTopicPartitioner) PartitionByBackup(r *kgo.Record, n int, backupIter kgo.TopicBackupIter) int {
	if r.Partition >= 0 {
		return int(r.Partition)
	}
	if fallback, ok := p.fallback.(kgo.TopicBackupPartitioner); ok {
		return fallback.PartitionByBackup(r, n, backupIter)
	}
	return p.fallback.Partition(r, n)
}

func (p explicitTopicPartitioner) OnNewBatch() {
	if fallback, ok := p.fallback.(kgo.TopicPartitionerOnNewBatch); ok {
		fallback.OnNewBatch()
	}
}
