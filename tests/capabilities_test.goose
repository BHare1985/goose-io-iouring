// Checks which capabilities the io_uring loop declares (contract 0.5): exactly half_close, sizing, abort, inspection, batching and name_resolution.
// Build and run:  ./build.sh tests/capabilities_test.goose /tmp/capabilities_test && /tmp/capabilities_test
import std;
import goose_io from "github.com/BHare1985/goose-io@apply-name-resolution";
import iouring from "..";

fn supplied<S>(s: S) -> bool {
    const if same<S, goose_io::Supplied> { return true; } else { return false; }
}

fn main() {
    var io = iouring::Loop { .. };
    assert(goose_io::VERSION_MAJOR == 0 && goose_io::VERSION_MINOR == 5);
    assert(supplied(goose_io::has_half_close(io)));
    assert(supplied(goose_io::has_sizing(io)));
    assert(supplied(goose_io::has_abort(io)));
    assert(supplied(goose_io::has_inspection(io)));
    assert(supplied(goose_io::has_batching(io)));
    assert(supplied(goose_io::has_shared_listen(io)));
    assert(supplied(goose_io::has_name_resolution(io)));
    assert(!supplied(goose_io::has_unix(io)));
    assert(!supplied(goose_io::has_relay(io)));
    assert(!supplied(goose_io::has_peek(io)));
    assert(!supplied(goose_io::has_revoke(io)));
    assert(!supplied(goose_io::has_wake(io)));
    assert(!supplied(goose_io::has_limits(io)));
    print("capabilities: 7 supplied, 6 absent");
}
