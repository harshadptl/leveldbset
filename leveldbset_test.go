package leveldbset

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_SetData(t *testing.T) {
	l, err0 := New("test")
	assert.NoError(t, err0, "make set without error")
	assert.NotNil(t, l, "set should be created")

	assert.True(t, l.IsEmpty(), "should be empty at the beginning")

	err := l.Add("testKey")
	assert.NoError(t, err, "key set without error")
	assert.False(t, l.IsEmpty(), "set not empty")
	assert.Equal(t, int64(1), l.Size(), "set size one")

	err1 := l.Remove("testKey")
	assert.NoError(t, err1, "key removed without error")
	assert.True(t, l.IsEmpty(), "set is now empty after removing key")
	assert.Equal(t, int64(0), l.Size(), "s")
}

func Test_PopData(t *testing.T) {
	l, err0 := New("test2")
	assert.NoError(t, err0, "make set without error")
	assert.NotNil(t, l, "set should be created")

	assert.True(t, l.IsEmpty(), "should be empty at the beginning")

	err := l.Add("testKey")
	assert.NoError(t, err, "key set without error")
	assert.False(t, l.IsEmpty(), "set not empty")
	assert.Equal(t, int64(1), l.Size(), "set size one")

	err = l.Add("testKey2")
	assert.NoError(t, err, "key set without error")
	assert.False(t, l.IsEmpty(), "set not empty")
	assert.Equal(t, int64(2), l.Size(), "set size two")

	err = l.Add("testKey3")
	assert.NoError(t, err, "key set without error")
	assert.False(t, l.IsEmpty(), "set not empty")
	assert.Equal(t, int64(3), l.Size(), "set size three")

	key, err1 := l.Pop()
	assert.NoError(t, err1, "key pop without error")
	assert.Equal(t, int64(2), l.Size(), "s")
	assert.Equal(t, "testKey", key)

	key2, err2 := l.Pop()
	assert.NoError(t, err2, "key pop without error")
	assert.Equal(t, int64(1), l.Size(), "s")
	assert.Equal(t, "testKey2", key2)

	key3, err3 := l.Pop()
	assert.NoError(t, err3, "key pop without error")
	assert.Equal(t, int64(0), l.Size(), "s")
	assert.Equal(t, "testKey3", key3)

	key4, err4 := l.Pop()
	assert.Error(t, err4, "key pop with error")
	assert.Equal(t, ErrSetEmpty, err4)
	assert.Equal(t, int64(0), l.Size(), "s")
	assert.Equal(t, "", key4)
}
