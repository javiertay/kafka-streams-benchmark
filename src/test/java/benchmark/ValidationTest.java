package benchmark;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ValidationTest {
    @Test void exactOutputIsValid() {
        assertTrue(new Validation(10, 10, 10, 10, 10, 0, 0, 0, 0).valid());
    }

    @Test void missingDuplicateAndUnexpectedOutputInvalidateRun() {
        assertFalse(new Validation(10, 10, 10, 9, 9, 1, 0, 0, 0).valid());
        assertFalse(new Validation(10, 10, 10, 11, 10, 0, 1, 0, 0).valid());
        assertFalse(new Validation(10, 10, 10, 10, 10, 0, 0, 1, 0).valid());
        assertFalse(new Validation(10, 10, 9, 10, 10, 0, 0, 0, 0).valid());
        assertFalse(new Validation(10, 10, 10, 10, 10, 0, 0, 0, 1).valid());
    }
}
