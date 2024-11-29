### 2024/10/24 finish lab1

### 2024/10/27 finish lab2

### 2024/11/ finish lab3

#### lab3优化：

1. 发送rpc时通常不持有锁。
   * 并发性：Raft 需要能够并行处理多个客户端的请求，因此在发送 RPC 时持有锁会限制并发性，降低系统的吞吐量。
   * 延迟：RPC 调用可能会因为网络延迟而阻塞，持有锁会导致其他操作无法执行，从而增加响应时间。
   * 死锁风险：持有锁可能会引入死锁的风险，特别是在多个节点之间进行 RPC 通信时。

2. 当日志条目应用到状态机时，通常也不持有锁。

   - **异步处理**：Raft 通常使用异步方式将日志条目应用到状态机。这意味着可以在不阻塞其他操作的情况下进行应用，允许系统同时处理多个请求。

   - **提高吞吐量**：不持有锁可以避免在应用日志条目时造成的性能瓶颈，从而提高整体吞吐量和响应速度。

   - **避免死锁**：持有锁可能会导致死锁，特别是在处理多个并发请求时。通过避免在应用日志时持锁，可以降低这种风险。

   - **使用信号量或通道**：在许多实现中，Raft 会使用信号量或通道来进行状态更新和通知。这种方式能够在保证一致性的同时，允许其他操作继续进行。

3. 在通过所有测试后，对比自己和其他人的用时，发现在倒数第二个测试点耗时过于长，思考原因如下：
   当客户端连续调用多次`start`函数时，如果每次调用`start`函数都要发起一次日志同步，会导致发送几十次日志同步，
   而在每次日志同步调用`appendEntries`时请求所包含的日志条目基本一致，这样会造成不必要的浪费。 

    解决方法：将日志同步的触发与上层服务提交新指令解耦，这样能够大幅度减少数据的传输量rpc次数和系统调用。

    优化实现参考`sofajraft`的日志复制实现。

4. 