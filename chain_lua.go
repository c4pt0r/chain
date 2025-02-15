package chain

import (
	"context"
	"fmt"

	lua "github.com/yuin/gopher-lua"
)

// LuaLoader registers the chain library to Lua state
func LuaLoader(L *lua.LState) int {
	// Initialize the stream type first
	mt := L.NewTypeMetatable("stream_mt")

	// Create methods table
	methods := L.NewTable()
	L.SetFuncs(methods, map[string]lua.LGFunction{
		"map":      streamMap,
		"filter":   streamFilter,
		"reduce":   streamReduce,
		"foreach":  streamForEach,
		"collect":  streamCollect,
		"parallel": streamParallel,
	})

	// Set methods
	L.SetField(mt, "__index", methods)

	// Create the module table
	mod := L.NewTable()
	L.SetFuncs(mod, map[string]lua.LGFunction{
		"new":       newStream,
		"generator": newGenerator,
	})

	// Store the metatable in the registry for later use
	L.SetField(mod, "_mt", mt)

	// Register the module
	L.Push(mod)
	return 1 // Return the module table
}

// streamUserData wraps a Stream for Lua
type streamUserData struct {
	stream Stream[lua.LValue, lua.LValue]
}

// newStream creates a new stream from a Lua table
func newStream(L *lua.LState) int {
	tbl := L.CheckTable(1)
	slice := make([]lua.LValue, 0, tbl.Len())
	tbl.ForEach(func(_, value lua.LValue) {
		slice = append(slice, value)
	})

	// Create stream
	stream := NewSliceStream(slice)
	ud := L.NewUserData()
	ud.Value = &streamUserData{stream: stream}

	// Get the metatable from the module
	mod := L.GetGlobal("chain").(*lua.LTable)
	mt := mod.RawGetString("_mt").(*lua.LTable)
	L.SetMetatable(ud, mt)

	L.Push(ud)
	return 1
}

// streamMap implements Stream.Map
func streamMap(L *lua.LState) int {
	ud := checkStream(L)
	fn := L.CheckFunction(2)

	mapped := ud.stream.Map(func(v lua.LValue) lua.LValue {
		L.Push(fn)
		L.Push(v)
		if err := L.PCall(1, 1, nil); err != nil {
			return lua.LNil
		}
		result := L.Get(-1)
		L.Pop(1) // Clean up the stack
		return result
	})

	newUD := L.NewUserData()
	newUD.Value = &streamUserData{stream: mapped}
	L.SetMetatable(newUD, L.GetMetatable(L.Get(1)))
	L.Push(newUD)
	return 1
}

// streamFilter implements Stream.Filter
func streamFilter(L *lua.LState) int {
	ud := checkStream(L)
	fn := L.CheckFunction(2)

	filtered := ud.stream.Filter(func(v lua.LValue) bool {
		L.Push(fn)
		L.Push(v)
		if err := L.PCall(1, 1, nil); err != nil {
			return false
		}
		result := lua.LVAsBool(L.Get(-1))
		L.Pop(1) // Clean up the stack
		return result
	})

	newUD := L.NewUserData()
	newUD.Value = &streamUserData{stream: filtered}
	L.SetMetatable(newUD, L.GetMetatable(L.Get(1)))
	L.Push(newUD)
	return 1
}

// streamReduce implements Stream.Reduce
func streamReduce(L *lua.LState) int {
	ud := checkStream(L)
	fn := L.CheckFunction(2)

	result, err := ud.stream.Reduce(func(a, b lua.LValue) lua.LValue {
		L.Push(fn)
		L.Push(a)
		L.Push(b)
		if err := L.PCall(2, 1, nil); err != nil {
			return lua.LNil
		}
		result := L.Get(-1)
		L.Pop(1) // Clean up the stack
		return result
	})

	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	L.Push(result)
	return 1
}

// streamForEach implements Stream.ForEach
func streamForEach(L *lua.LState) int {
	ud := checkStream(L)
	fn := L.CheckFunction(2)

	err := ud.stream.ForEach(func(v lua.LValue) {
		L.Push(fn)
		L.Push(v)
		if err := L.PCall(1, 0, nil); err != nil {
			// Handle error if needed
			fmt.Println("Error in ForEach:", err)
		}
	})

	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	return 0
}

// streamCollect implements Stream.Collect
func streamCollect(L *lua.LState) int {
	ud := checkStream(L)

	result, err := ud.stream.Collect(context.Background())
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	// Convert result to Lua table
	tbl := L.CreateTable(len(result), 0)
	for i, v := range result {
		tbl.RawSetInt(i+1, v)
	}

	L.Push(tbl)
	return 1
}

// streamParallel implements Stream.Parallel which enables concurrent processing
// workers parameter determines the number of goroutines used for parallel execution
func streamParallel(L *lua.LState) int {
	ud := checkStream(L)
	workers := L.CheckInt(2)

	parallel := ud.stream.Parallel(workers)
	newUD := L.NewUserData()
	newUD.Value = &streamUserData{stream: parallel}
	L.SetMetatable(newUD, L.GetMetatable(L.Get(1)))
	L.Push(newUD)
	return 1
}

// newGenerator creates a new stream from a Lua generator function
// The generator function should return (value, continue) pairs
func newGenerator(L *lua.LState) int {
	fn := L.CheckFunction(1)

	gen := func() (lua.LValue, bool) {
		L.Push(fn)
		L.Call(0, 2)
		value := L.Get(-2)
		ok := lua.LVAsBool(L.Get(-1))
		L.Pop(2)
		return value, ok
	}

	stream := Generator(gen)
	ud := L.NewUserData()
	ud.Value = &streamUserData{stream: stream}
	L.SetMetatable(ud, L.GetTypeMetatable("stream_mt"))
	L.Push(ud)
	return 1
}

// Helper function to check and get stream userdata
func checkStream(L *lua.LState) *streamUserData {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*streamUserData); ok {
		return v
	}
	L.ArgError(1, "stream expected")
	return nil
}
