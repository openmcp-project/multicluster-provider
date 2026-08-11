package provider

import (
	"k8s.io/apimachinery/pkg/labels"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
)

type Options struct {
	AccessRequestSelectors []labels.Selector
}

// Option encapsulates a configuration option for the provider.
type Option interface {
	Apply(*Options)
}

// DynamicOption is an Option which can also be applied during runtime, while the provider is running.
type DynamicOption interface {
	Option

	// ApplyDynamic applies the option to the provider while it is running.
	ApplyDynamic(*Options) error
}

// DeepCopy returns a deep copy of the Options object.
func (o *Options) DeepCopy() *Options {
	if o == nil {
		return nil
	}

	res := &Options{
		AccessRequestSelectors: make([]labels.Selector, len(o.AccessRequestSelectors)),
	}

	for i, sel := range o.AccessRequestSelectors {
		res.AccessRequestSelectors[i] = sel.DeepCopySelector()
	}

	return res
}

type optionFunc struct {
	ApplyFunc func(*Options)
}

func (o optionFunc) Apply(opts *Options) {
	if o.ApplyFunc == nil {
		return
	}
	o.ApplyFunc(opts)
}

// OptionFunc can be used to wrap a function into an Option without having to define a new type.
func OptionFunc(apply func(*Options)) Option {
	return optionFunc{
		ApplyFunc: apply,
	}
}

type dynamicOptionFunc struct {
	optionFunc

	ApplyDynamicFunc func(*Options) error
}

func (o dynamicOptionFunc) ApplyDynamic(opts *Options) error {
	if o.ApplyDynamicFunc == nil {
		return nil
	}
	return o.ApplyDynamicFunc(opts)
}

// DynamicOptionFunc can be used to wrap a function into a DynamicOption without having to define a new type.
// The applyDynamic function may be nil, in which case the static apply func is used for both static and dynamic application of the option (always returning nil for the dynamic application).
func DynamicOptionFunc(apply func(*Options), applyDynamic func(*Options) error) DynamicOption {
	res := dynamicOptionFunc{
		optionFunc: optionFunc{
			ApplyFunc: apply,
		},
		ApplyDynamicFunc: applyDynamic,
	}
	if res.ApplyDynamicFunc == nil {
		res.ApplyDynamicFunc = func(opts *Options) error {
			res.ApplyFunc(opts)
			return nil
		}
	}
	return res
}

// WithAccessRequestSelectors sets the AccessRequest label selectors.
// Note that this does not additively append to the existing selectors, but replaces them entirely.
// Also note that if the list of selectors is empty, this will result in no AccessRequests being matched, so this option should be used.
func WithAccessRequestSelectors(selectors ...labels.Selector) DynamicOption {
	return DynamicOptionFunc(func(opts *Options) {
		opts.AccessRequestSelectors = make([]labels.Selector, len(selectors))
		copy(opts.AccessRequestSelectors, selectors)
	}, nil)
}

// MatchesAnyAccessRequestSelector returns true if the given AccessRequest matches any of the access request selectors.
// If the list of access request selectors is empty, this function will always return false.
func (o *Options) MatchesAnyAccessRequestSelector(ar *clustersv1alpha1.AccessRequest) bool {
	for _, sel := range o.AccessRequestSelectors {
		if sel.Matches(labels.Set(ar.GetLabels())) {
			return true
		}
	}

	return false
}
