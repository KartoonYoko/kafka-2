package kafka

type DeserializationError struct {
    Err error
}

func NewDeserializationError(err error) *DeserializationError {
    return &DeserializationError{
        Err: err,
    }
}

func (e *DeserializationError) Error() string {
    return "deserialization error: " + e.Err.Error()
}

func (e *DeserializationError) Unwrap() error {
    return e.Err
}