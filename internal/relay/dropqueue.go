package relay

func EnqueueDropOldest[T any](ch chan T, item T) int {
	select {
	case ch <- item:
		return 0
	default:
	}

	dropped := 0
	select {
	case <-ch:
		dropped = 1
	default:
	}

	select {
	case ch <- item:
		return dropped
	default:
		return dropped + 1
	}
}
