package utb.fai;

import java.io.*;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;

public class SocketHandler {
	/** mySocket is the socket this SocketHandler takes care of */
	Socket mySocket;

	/** client ID is a string in the format <IP_address>:<port> */
	String clientID;

	/**
	 * activeHandlers is a reference to the set of all running SocketHandlers.
	 * We need it to be able to send messages from this client to the others.
	 */
	ActiveHandlers activeHandlers;

	/**
	 * messages is the queue of outgoing messages of this client - if the
	 * client's network is slow, messages wait here for delivery.
	 */
	ArrayBlockingQueue<String> messages = new ArrayBlockingQueue<String>(20);

	/**
	 * startSignal makes sure both tasks OutputHandler.run() and
	 * InputHandler.run() start at the same moment.
	 */
	CountDownLatch startSignal = new CountDownLatch(2);

	/** outputHandler.run() takes care of the OutputStream of my socket */
	OutputHandler outputHandler = new OutputHandler();
	/** inputHandler.run() takes care of the InputStream of my socket */
	InputHandler inputHandler = new InputHandler();
	/**
	 * the outputHandler cannot detect that the socket was closed,
	 * inputFinished tells it
	 */
	volatile boolean inputFinished = false;

	/** user name, null until the client sets it */
	volatile String name = null;

	/** rooms the client is in (keeps insertion order, thread safe) */
	final Set<String> rooms = new CopyOnWriteArraySet<String>();

	public SocketHandler(Socket mySocket, ActiveHandlers activeHandlers) {
		this.mySocket = mySocket;
		clientID = mySocket.getInetAddress().toString() + ":" + mySocket.getPort();
		this.activeHandlers = activeHandlers;
		rooms.add("public");
	}

	/** puts a message to the queue of this client, drops it if the queue is full */
	void deliver(String message) {
		if (!messages.offer(message))
			System.err.printf("Client %s message queue is full, dropping the message!\n", clientID);
	}

	/** true if this client and the other one are in at least one common room */
	boolean sharesRoomWith(SocketHandler other) {
		for (String room : rooms)
			if (other.rooms.contains(room))
				return true;
		return false;
	}

	// ------------------------------------------------------------ commands

	private void processLine(String line) {
		if (line.isEmpty())
			return;

		if (line.startsWith("#")) {
			processCommand(line);
			return;
		}

		if (name == null) {
			// the first line after connecting is the name
			setName(line);
			return;
		}

		activeHandlers.sendMessageToRooms(this, "[" + name + "] >> " + line);
	}

	private void processCommand(String line) {
		String[] parts = line.split("\\s+", 2);
		String command = parts[0];
		String rest = parts.length > 1 ? parts[1].trim() : "";

		if (command.equals("#setMyName")) {
			setName(rest);
			return;
		}

		if (name == null) {
			deliver("You must set your name first. Please enter your name:");
			return;
		}

		if (command.equals("#sendPrivate")) {
			String[] p = rest.split("\\s+", 2);
			if (rest.isEmpty() || p.length < 2 || p[1].trim().isEmpty()) {
				deliver("Usage: #sendPrivate <name> <message>");
				return;
			}
			if (!activeHandlers.sendPrivate(p[0], "[" + name + "] >> " + p[1].trim()))
				deliver("User " + p[0] + " does not exist.");
		} else if (command.equals("#join")) {
			if (rest.isEmpty()) {
				deliver("Usage: #join <room>");
			} else if (rooms.add(rest)) {
				deliver("You joined room " + rest + ".");
			} else {
				deliver("You are already in room " + rest + ".");
			}
		} else if (command.equals("#leave")) {
			if (rest.isEmpty()) {
				deliver("Usage: #leave <room>");
			} else if (rooms.remove(rest)) {
				deliver("You left room " + rest + ".");
			} else {
				deliver("You are not in room " + rest + ".");
			}
		} else if (command.equals("#groups")) {
			deliver(String.join(",", rooms));
		} else {
			deliver("Unknown command " + command + ".");
		}
	}

	private void setName(String newName) {
		if (newName.isEmpty() || newName.startsWith("#") || newName.matches(".*\\s.*")) {
			deliver("Invalid name. The name must not be empty or contain spaces. Please enter your name:");
		} else if (!activeHandlers.registerName(this, newName)) {
			deliver("Name " + newName + " is already taken. Please choose another one:");
		} else {
			deliver("Your name is " + newName + ".");
		}
	}

	// --------------------------------------------------------------- tasks

	class OutputHandler implements Runnable {
		public void run() {
			OutputStreamWriter writer;
			try {
				System.err.println("DBG>Output handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Output handler running for " + clientID);
				writer = new OutputStreamWriter(mySocket.getOutputStream(), "UTF-8");
				writer.write("\nYou are connected from " + clientID + "\n");
				writer.flush();
				while (!inputFinished) {
					String m = messages.take();// blocking read - sleeps when the queue is empty
					if (inputFinished)
						break;
					writer.write(m + "\r\n"); // send messages from others to our client
					writer.flush();
					System.err.println("DBG>Message sent to " + clientID + ":" + m + "\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				try {
					mySocket.close();
				} catch (IOException e) {
					// nothing to do
				}
			}
			System.err.println("DBG>Output handler for " + clientID + " has finished.");

		}
	}

	class InputHandler implements Runnable {
		public void run() {
			try {
				System.err.println("DBG>Input handler starting for " + clientID);
				startSignal.countDown();
				startSignal.await();
				System.err.println("DBG>Input handler running for " + clientID);
				String request = "";
				/*
				 * The handler is added to activeHandlers by App right after accept(),
				 * so that the limit of connections can be checked reliably.
				 */
				messages.offer("Please enter your name:");
				BufferedReader reader = new BufferedReader(new InputStreamReader(mySocket.getInputStream(), "UTF-8"));
				while ((request = reader.readLine()) != null) { // did my client send something?
					request = request.trim();
					System.out.println("From client " + clientID + ": " + request);
					processLine(request);
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				// remove yourself from the set of activeHandlers
				synchronized (activeHandlers) {
					activeHandlers.remove(SocketHandler.this);
				}
				inputFinished = true;
				// make room in the queue and wake the output handler up
				messages.clear();
				messages.offer("OutputHandler, wakeup and die!");
			}
			System.err.println("DBG>Input handler for " + clientID + " has finished.");
		}

	}
}