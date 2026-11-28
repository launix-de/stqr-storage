// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func checkPlannerStatistics(source []byte) error {
	tree, err := parser.ParseFile(token.NewFileSet(), "planner.go", source, 0)
	if err != nil {
		return err
	}
	for _, imp := range tree.Imports {
		if strings.Contains(imp.Path.Value, "/internal/decay") {
			return fmt.Errorf("planner owns a decay implementation")
		}
	}
	var violation error
	ast.Inspect(tree, func(n ast.Node) bool {
		field, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch field.Sel.Name {
		case "Tick", "Observe", "SeedRate", "AddInterval", "Rates", "Forecast", "CollectStats", "readWindow", "writeWindow", "growthWindow", "readCounter", "writeCounter", "lastRead", "lastWrite":
			violation = fmt.Errorf("planner samples or accesses mutable statistic %s", field.Sel.Name)
		}
		return true
	})
	return violation
}
func TestTelemetryPlannerArchitecture(t *testing.T) {
	// Prove the detector rejects a valid Go sampler, not only missing text.
	if err := checkPlannerStatistics([]byte("package server;func plan(){counter.Tick(now,halfLife)}")); err == nil || !strings.Contains(err.Error(), "Tick") {
		t.Fatal("sampler positive control not detected", err)
	}
	if err := checkPlannerStatistics([]byte("package server;func plan(){stats.volumeForecast(node,boot,id,now)}")); err != nil {
		t.Fatal("read-only control rejected", err)
	}
	files, err := filepath.Glob("placement*.go")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, "range_balance.go", "range_planner.go", "pg_rebuild_workers.go")
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") || name == "placement_settings.go" {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err = checkPlannerStatistics(source); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	planner := reflect.TypeOf(rangeManager{})
	for _, name := range []string{"metrics", "readers", "measurements", "capacityMoveMean", "capacityMoveTime"} {
		if _, ok := planner.FieldByName(name); ok {
			t.Errorf("planner retains private measurement %s", name)
		}
	}
}
