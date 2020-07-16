package ekv



// NextUntil applies FnKeyCmp to each entry of the iterator until meets some condition.
// It will stop when fn returns true, or iterator is invalid or an error occurs.
func NextUntil(it Iterator, fn FnKeyCmp) error {
	var err error
	for it.Valid() && !fn(it.Key()) {
		err = it.Next()
		if err != nil {
			return err
		}
	}
	return nil
}
