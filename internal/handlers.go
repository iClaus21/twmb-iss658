package internal

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

func (v *Kafka) onPartitionsAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			v.Log().
				WithFields(logrus.Fields{"topic": topic}).
				WithFields(logrus.Fields{"partition": partition}).
				Info("Partition assigned")
		}
	}
}

func (v *Kafka) onPartitionsLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	for topic, partitions := range lost {
		for _, partition := range partitions {
			v.Log().
				WithFields(logrus.Fields{"topic": topic}).
				WithFields(logrus.Fields{"partition": partition}).
				Info("Partition lost")
		}
	}
}

func (v *Kafka) onPartitionsRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	for topic, partitions := range revoked {
		for _, partition := range partitions {
			v.Log().
				WithFields(logrus.Fields{"topic": topic}).
				WithFields(logrus.Fields{"partition": partition}).
				Info("Partition revoked")
		}
	}
}
