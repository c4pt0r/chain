package chain

import (
	"reflect"
	"sort"
	"testing"

	lua "github.com/yuin/gopher-lua"
)

func setupLuaState(t *testing.T) *lua.LState {
	L := lua.NewState()

	// Register the loader
	L.PreloadModule("chain", func(L *lua.LState) int {
		// Create and initialize the module
		return LuaLoader(L)
	})

	// Load the chain module and set it as global
	if err := L.DoString(`
		local m = require("chain")
		if m == nil then
			error("Failed to load chain module")
		end
		chain = m  -- Set module as global
	`); err != nil {
		t.Fatalf("Failed to load chain module: %v", err)
	}

	return L
}

func TestLuaStreamBasics(t *testing.T) {
	L := setupLuaState(t)
	defer L.Close()

	// Run basic stream operations
	err := L.DoString(`
		-- Create a stream from a table
		local s = chain.new({1, 2, 3, 4, 5})
		
		-- Map operation
		local doubled = s:parallel(3):map(function(x)
			return x * 2
		end)
		
		-- Collect results
		local result = doubled:collect()

		-- sort the results
		table.sort(result)
		
		-- Store results in global variable for testing
		results = {}
		for i, v in ipairs(result) do
			results[i] = v
		end
	`)

	if err != nil {
		t.Fatalf("Failed to execute Lua code: %v", err)
	}

	// Check results
	results := L.GetGlobal("results").(*lua.LTable)
	expected := []int{2, 4, 6, 8, 10}

	for i, expect := range expected {
		val := results.RawGetInt(i + 1)
		if val.String() != lua.LNumber(expect).String() {
			t.Errorf("at index %d: expected %d, got %s", i, expect, val)
		}
	}
}

func TestLuaStreamChaining(t *testing.T) {
	L := setupLuaState(t)
	defer L.Close()

	err := L.DoString(`
		local s = chain.new({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
		
		-- Chain multiple operations
		local result = s
			:filter(function(x) return x % 2 == 0 end)  -- keep even numbers
			:map(function(x) return x * x end)          -- square them
			:filter(function(x) return x > 20 end)      -- keep values > 20
			:collect()
			
		-- Store results for testing
		results = {}
		for i, v in ipairs(result) do
			results[i] = v
		end
	`)

	if err != nil {
		t.Fatalf("Failed to execute Lua code: %v", err)
	}

	results := L.GetGlobal("results").(*lua.LTable)
	expected := []int{36, 64, 100}

	var actual []int
	results.ForEach(func(_, value lua.LValue) {
		actual = append(actual, int(value.(lua.LNumber)))
	})

	if len(actual) != len(expected) {
		t.Errorf("expected length %d, got %d", len(expected), len(actual))
	}

	// Compare results (order might vary due to parallel execution)
	found := make(map[int]bool)
	for _, v := range actual {
		found[v] = true
	}

	for _, expect := range expected {
		if !found[expect] {
			t.Errorf("expected value %d not found in results", expect)
		}
	}
}

func TestLuaGenerator(t *testing.T) {
	L := setupLuaState(t)
	defer L.Close()

	err := L.DoString(`
		local count = 0
		local stream = chain.generator(function()
			count = count + 1
			if count <= 3 then
				return count, true
			end
			return nil, false
		end)
		
		results = stream:collect()
	`)

	if err != nil {
		t.Fatalf("Failed to execute Lua code: %v", err)
	}

	results := L.GetGlobal("results").(*lua.LTable)
	expected := []int{1, 2, 3}

	for i, expect := range expected {
		val := results.RawGetInt(i + 1)
		if val.String() != lua.LNumber(expect).String() {
			t.Errorf("at index %d: expected %d, got %s", i, expect, val)
		}
	}
}

func TestLuaReduce(t *testing.T) {
	L := setupLuaState(t)
	defer L.Close()

	err := L.DoString(`
		local s = chain.new({1, 2, 3, 4, 5})
		
		result = s:reduce(function(a, b)
			return a + b
		end)
	`)

	if err != nil {
		t.Fatalf("Failed to execute Lua code: %v", err)
	}

	result := L.GetGlobal("result")
	expected := lua.LNumber(15)

	if result.String() != expected.String() {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestLuaForEach(t *testing.T) {
	L := setupLuaState(t)
	defer L.Close()

	err := L.DoString(`
		sum = 0
		local s = chain.new({1, 2, 3, 4, 5})
		
		s:foreach(function(x)
			sum = sum + x
		end)
	`)

	if err != nil {
		t.Fatalf("Failed to execute Lua code: %v", err)
	}

	sum := L.GetGlobal("sum")
	expected := lua.LNumber(15)

	if sum.String() != expected.String() {
		t.Errorf("expected %v, got %v", expected, sum)
	}
}

// Add a new test specifically for parallel operations
func TestLuaStreamParallel(t *testing.T) {
	L := setupLuaState(t)
	defer L.Close()

	err := L.DoString(`
		local s = chain.new({1, 2, 3, 4, 5})
		
		local result = s
			:parallel(3)
			:map(function(x) return x * 2 end)
			:collect()
			
		results = {}
		for i, v in ipairs(result) do
			results[i] = v
		end
	`)

	if err != nil {
		t.Fatalf("Failed to execute Lua code: %v", err)
	}

	results := L.GetGlobal("results").(*lua.LTable)
	expected := []int{2, 4, 6, 8, 10}

	var actual []int
	results.ForEach(func(_, value lua.LValue) {
		actual = append(actual, int(value.(lua.LNumber)))
	})

	// Sort both slices for comparison since parallel execution may change order
	sort.Ints(actual)
	sort.Ints(expected)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}
