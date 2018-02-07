package bufmgr;

class Clock {
    int current;

    public Clock() {
        current = 0;
    }

    public int pickVictim(FrameDesc[] frameTab) {
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
        int N = frameTab.length;
        for(int counter = 0; counter < N*2; counter++) {
            if(!frameTab[current].valid) return current;
            if(frameTab[current].pinCount < 1) {
                if(frameTab[current].refBit) frameTab[current].refBit = false;
                else return current;
            }
            current = (current + 1) % N;
        }
        throw new IllegalStateException("No victim found.");
    }
}
