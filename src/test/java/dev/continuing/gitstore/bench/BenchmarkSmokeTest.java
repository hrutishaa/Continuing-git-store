package dev.continuing.gitstore.bench;

import org.junit.jupiter.api.Test;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkSmokeTest {
    @Test
    void miniRunPrintsMeasuredRatesAndHoldsInvariants() {
        var buffer = new StringWriter();
        int code = BenchmarkRunner.run(BenchmarkRunner.Plan.mini(), buffer, null);
        String report = buffer.toString();
        assertEquals(0, code, report);
        assertTrue(report.contains("pushes_per_sec="), report);
        assertTrue(report.contains("invariant_single_winner=true"), report);
        assertTrue(report.contains("source=BenchmarkRunner"), report);
        assertFalse(report.contains("history_ok=false"), report);
        assertFalse(report.contains("FAILED="), report);
    }
}
