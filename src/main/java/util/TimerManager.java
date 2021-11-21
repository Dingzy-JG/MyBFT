package util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class TimerManager {

    private volatile static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(300);

    // 延迟执行
    public static void schedule(Supplier<?> action, long delay){
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                action.get();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    // 在一定延时(initialDelay)之后，开始周期性的运行Runnable任务。
    // 周期性：每过一段时间(period)，就开始运行一次Runnable任务。
    public static void scheduleAtFixedRate(Supplier<?> action,long initialDelay, long period ){
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                action.get();
            }
        }, initialDelay,period, TimeUnit.MILLISECONDS);
    }

    // 在一定延时(initialDelay)之后，开始周期性的运行Runnable任务
    // 周期性：上一次任务执行完成之后，等待一段时间(delay)，然后开始下一次任务
    // 执行完之后再过一段时间再执行
    public static void scheduleWithFixedDelay(Supplier<?> action,long initialDelay, long period ){
        executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                action.get();
            }
        }, initialDelay,period, TimeUnit.MILLISECONDS);
    }

}
