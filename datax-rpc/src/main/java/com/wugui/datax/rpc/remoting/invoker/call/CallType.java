package com.wugui.datax.rpc.remoting.invoker.call;

/**
 * rpc call type
 *
 * @author xuxueli 2018-10-19
 */
public enum CallType {


    SYNC, //同步，客户端发起调用后，当前线程会被阻塞，直到等待服务端返回结果或超时异常，再进行后续操作

    FUTURE, //异步，不用同步等待服务端的结果，获取 RPC 框架给的 Future 对象，不会阻塞线程，继续执行后面逻辑。服务端返回响应结果被 RPC 缓存，当客户端需要时主动获取结果，获取结果的过程阻塞线程。

    CALLBACK,//回调，和future的区别在于，future的返回结果是在当前线程中进行获取的，而callback是会新建一个线程，用来处理返回结果。

    ONEWAY;//单向调用，客户端不等待并忽略返回结果。一次调用，不需要返回，客户端线程请求发出即结束，立刻释放线程资源。


    //试图匹配名字为name的CallType,如果在此枚举类找不到，则返回defaultCallType
    public static CallType match(String name, CallType defaultCallType){
        for (CallType item : CallType.values()) {
            if (item.name().equals(name)) {
                return item;
            }
        }
        return defaultCallType;
    }

}
