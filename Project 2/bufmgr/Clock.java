/* Minibase Implementation for CS 487/587
 * Docs: http://pages.cs.wisc.edu/~dbbook/openAccess/Minibase/minibase.html
 * Authors: Alexander Goddard & Geoff Maggi
 */

package bufmgr;

/**
 * Implements the second change clock algorithm
 */
class Clock {
	int current;

	public Clock() {
		current = 0;
	}

	public int pickVictim(FrameDesc[] bufPool) {
		int N = bufPool.length;
		for (int counter = 0; counter < N * 2; counter++) {
			if (!bufPool[current].valid)
				return current;
			if (bufPool[current].pinCount < 1) {
				if (bufPool[current].refbit)
					bufPool[current].refbit = false;
				else
					return current;
			}
			current = (current + 1) % N;
		}
		throw new IllegalStateException("No victim found.");
	}
}