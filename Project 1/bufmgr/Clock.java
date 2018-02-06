package bufmgr;

class Clock {
	int current;
	
	public Clock() {
			current = 0;
	}
	
	public int pickVictim(FrameDesc[] bufPool) {
		/*
		 for(int counter = 0; counter < N*2; counter++) {
			if data in bufpool[current] is not valid, choose current;
			if frametab[current]'s pin count is 0 {
				if frametab [current] has refbit == true {
					set frametab [current]'s refbit = false
				} else {
					return current
				}
			}
			increment current, mod N
		}
		We couldn't find an available frame, so return an error
		*/
		int N = bufPool.length;
		for(int counter = 0; counter < N*2; counter++) {
			if(!bufPool[current].valid()) return current;
			if(bufPool[current].pinCount < 1) {
				if(bufPool[current].refbit) bufPool[current].refbit = false;
				else return current;
			}
			current = (current + 1) % N;
		}
		throw new IllegalStateException("No victim found.");
	}
}