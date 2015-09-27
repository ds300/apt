package uk.ac.susx.tag.apt.tasks;

import java.lang.reflect.Method;

/**
 * Created by ds300 on 18/09/2015.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String command = args[0];

        String[] actualArgs = new String[args.length-1];
        System.arraycopy(args, 1, actualArgs, 0, actualArgs.length);

        switch (command) {
            case "clj":
                Class mainClass = Class.forName("tag.apt.main");
                Method method = mainClass.getMethod("main", String[].class);
                method.invoke(null, new Object[]{actualArgs});
                break;
            case "construct":
                Construct.main(actualArgs);
                break;
            case "compose":
                Compose.main(actualArgs);
                break;
            case "vectors":
                Vectors.main(actualArgs);
                break;
            default:
                System.err.println("no task: " + command);
                System.exit(1);
        }
    }
}
