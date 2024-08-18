package gash.grpc.route.server;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Worker extends Thread {

	private LinkedBlockingDeque<Work> _queue;

	public Worker(LinkedBlockingDeque<Work> queue) {
		this._queue = queue;
	}

	private void doWork(Work w) {

		if (w != null) {
			if (w.request.getDestination() == _serverID) {
				try {

					System.out.println("*** doWork() ***");
					// simulates latency
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// forward the message to Link instances
				for (var link : _links) {
					if ( link.getID == request.getDestination()) {
						send directly();
						break;
					}
				}
				
				// if no direct destination exists, forward to all 
				// links or the next link?
			}
		}
	}

	@Override
	public void run() {
		// TODO not a good idea to spin on work --> wastes CPU cycles
		while (true) {
			System.out.println("worker qsize = " + _queue.size());

			Work rtn = null;
			try {
				System.out.println("-- checking for work, qsize = " + _queue.size());
				rtn = _queue.poll(2, TimeUnit.SECONDS);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			doWork(rtn);
		}
	}
}
