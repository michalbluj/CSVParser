package com.company.caesars.generator.concurrent;

import java.util.concurrent.Executor;

/**
 * Created by Michal Bluj on 2017-06-23.
 */
public class SQLInsertExecutor implements Executor {
    public void execute(Runnable r) {
        r.run();
    }
}
