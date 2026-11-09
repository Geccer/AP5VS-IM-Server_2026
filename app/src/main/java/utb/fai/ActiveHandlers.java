package utb.fai;

import java.util.*;

public class ActiveHandlers {
    private static final long serialVersionUID = 1L;
    private HashSet<SocketHandler> activeHandlersSet = new HashSet<SocketHandler>();

    /**
     * sendMessageToAll - sends the message to all active clients except the sender
     *
     * @param sender  - reference to the sender
     * @param message - message text
     */
    synchronized void sendMessageToAll(SocketHandler sender, String message) {
        for (SocketHandler handler : activeHandlersSet)
            if (handler != sender) {
                handler.deliver(message);
            }
    }

    /**
     * sendMessageToRooms - sends the message to every client (except the sender)
     * that already has a name and shares at least one room with the sender.
     */
    synchronized void sendMessageToRooms(SocketHandler sender, String message) {
        for (SocketHandler handler : activeHandlersSet)
            if (handler != sender && handler.name != null && handler.sharesRoomWith(sender)) {
                handler.deliver(message);
            }
    }

    /**
     * sendPrivate - sends the message to the client with the given name.
     *
     * @return false if no such client exists
     */
    synchronized boolean sendPrivate(String targetName, String message) {
        for (SocketHandler handler : activeHandlersSet)
            if (targetName.equals(handler.name)) {
                handler.deliver(message);
                return true;
            }
        return false;
    }

    /**
     * registerName - atomically sets the name of the handler, unless another
     * active client already uses it.
     *
     * @return true if the name was set (or was already the handler's name)
     */
    synchronized boolean registerName(SocketHandler handler, String newName) {
        for (SocketHandler other : activeHandlersSet)
            if (other != handler && newName.equals(other.name))
                return false;
        handler.name = newName;
        return true;
    }

    synchronized int size() {
        return activeHandlersSet.size();
    }

    /**
     * add adds a new handler to the set of active handlers.
     *
     * @param handler - handler to add
     * @return true if the set did not already contain the specified element.
     */
    synchronized boolean add(SocketHandler handler) {
        return activeHandlersSet.add(handler);
    }

    /**
     * remove removes the handler from the set of active handlers.
     *
     * @param handler - handler to remove
     * @return true if the set contained the specified element.
     */
    synchronized boolean remove(SocketHandler handler) {
        return activeHandlersSet.remove(handler);
    }
}