import replica.Result;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

public class ServletLogic {

    HashMap<String, Method> eventHandler = new HashMap<>();

    public ServletLogic() {
        registerEventHandler("ADD", "add", EventHandler.class);        // Extract into constants
        registerEventHandler("GET", "get", EventHandler.class);        // Extract into constants
    }

    public <T> boolean registerEventHandler(String label, String methodName, Class<T> parent) {
        try {
            eventHandler.put(label, parent.getMethod(methodName));
            return true;
        } catch (NoSuchMethodException exception) {
            return false;
        }
    }

    public Result processRequest(String senderID, String requestLabel, String requestData) throws InvocationTargetException, IllegalAccessException {
        System.out.println("Processing request from: " + senderID);
        return (Result) eventHandler.get(requestLabel).invoke(requestData);
    }
}
