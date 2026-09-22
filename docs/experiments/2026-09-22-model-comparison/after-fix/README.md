# 修复部署后的对比复测

本轮在 agentd **0e50590f3a8bbd49ff0048336f79dcd590df6744** 部署后执行。原始受阻实验及其数据保持不变，见[上一轮报告](../README.md)。本轮完整结果见 [results.json](results.json)。

本轮 DeepSeek 完成 15/16，Qwen 完成 15/16；平均耗时分别为 2.49s 和 76.50s。Qwen 使用显式 `local/chat`，默认路由仍受网关 busy 状态阻碍。单轮相近的完成数不代表两者总体质量相同，延迟差异则在本组任务中很明显。

## 修复和部署

计算处理器改为读取已有工具 schema 声明的 `expression`，返回值继续使用已有的 `expr`、`result` 字段。回归测试通过真实工具目录、schema 校验及工具分发执行，已先验证旧代码会失败，再验证修复后成功；同时覆盖缺失参数、空白表达式、非法表达式和非有限结果。

82 项测试通过、3 项原有测试忽略；格式、workspace check、Clippy、部署脚本、demo provider、Dockerfile 检查通过。CI 和 Security 工作流通过；Clippy/Cargo 提示现有 nom 依赖的 future-incompatibility 提醒。

线上旧镜像已核实为 `18d1ffa3bf3cd32b83f5aef505ada964ee82cdfd`，更新至固定镜像：

```text
ghcr.io/minifish-org/agentd@sha256:ea00f8edd41882737a2cc9ac7084a23af61299474fc4b33668511881800ed767
```

更新前确认没有 running/queued runs，短暂停止 agentd，备份配置和数据后只重建 agentd 服务。备份位于 `/srv/agentd/deploy-backups/20260922T065432Z-0e50590`。新容器健康检查通过，tenant 列表保持一致，没有清空数据库、修改其他 agent 或重启其他服务。

主镜像发布成功。发布工作流整体仍因独立 `agentd-sandbox` 包的 GHCR `write_package` 权限错误失败；sandbox 推送失败不改变已发布、已验证并部署的 agentd 镜像，本次没有替换 sandbox 镜像或修改工作流权限。[发布记录](https://github.com/minifish-org/agentd/actions/runs/35696011324)。

## 网关问题与实验边界

部署后先用默认路由复测计算：DeepSeek 只调用一次工具成功（2.218s），Qwen 默认 `free/chat` 仍立即返回 503。网关健康端点同时显示 `local/chat` 为 `healthy=true`、`busy=true`、`in_flight=1`；原生推理日志显示最后任务已释放。自动路由会排除 busy 的模型，因此它可以报 `no_healthy_model`，即使模型服务是健康的。

现象符合取消后在途计数未清理，但具体取消缺陷尚未独立复现。没有修改或重启网关。为完成模型能力比较，本轮只把专用测试 agent 的 Qwen model 显式设为 **`local/chat`**，仍经过同一网关和 agentd 原生循环，后端仍返回 `local-llm`。这与前一轮“默认 free/chat”是两个不同配置，不能把网关可用性的改善归功于计算修复，也不能声称默认路由已恢复。

其余条件与原实验相同：16 个案例、temperature 0、max_tokens 2048、max_steps 8、180 秒 run 预算、context_window 0、相同四类工具和独立数据路径。逐例一次，先 DeepSeek 后 Qwen；没有重试替换失败分数、随机执行顺序或控制缓存。

## 结果

| 指标 | DeepSeek / standard/chat | Qwen / local/chat |
| --- | ---: | ---: |
| 目标完成 | 15/16 | 15/16 |
| 过程无偏差 | 13/16 | 15/16 |
| 工具选择符合任务 | 16/16 | 16/16 |
| 参数 schema 合规（不等于语义正确） | 26/26 | 25/25 |
| 计划内恢复目标完成 | 2/3 | 2/3 |
| 平均模型请求数 | 2.50 | 2.56 |
| 平均工具调用数 | 1.62 | 1.56 |
| 平均延迟 | 2.49s | 76.50s |
| 中位数 / p95 延迟 | 2.42s / 5.45s | 73.59s / 180.20s |
| 超出理想调用基线 | 3 | 2 |
| 相同失败调用再次出现 | 1 | 1 |

“目标完成”检查最终结构及要求的真实工具结果；artifact 必须落在指定路径。审阅本轮后加强了精确路径检查，并对原始 32 个 run 重算，上一轮所有目标完成结论保持不变。只在错误路径写入并读回相同文本不算成功。

“过程无偏差”还要求无检测到的参数/运行时契约错误，以及调用数不超过理想基线。预期的文件不存在、无效时区错误不算过程偏差。每例只有一个用户 turn，模型请求数不是用户轮次。

| 案例 | DeepSeek：结果 / 延迟 / 工具调用 | Qwen：结果 / 延迟 / 工具调用 |
| --- | --- | --- |
| `no_tool` | 完成 / 1.01s / 0 次 | 完成 / 26.98s / 0 次 |
| `extract` | 完成 / 0.82s / 0 次 | 完成 / 15.12s / 0 次 |
| `missing_context` | 完成 / 1.01s / 0 次 | 完成 / 16.52s / 0 次 |
| `boundary` | 完成 / 1.21s / 0 次 | 完成 / 17.93s / 0 次 |
| `calculate` | 完成 / 2.04s / 1 次 | 完成 / 27.59s / 1 次 |
| `calculation_chain` | 完成 / 2.23s / 2 次 | 完成 / 62.43s / 2 次 |
| `clock` | 完成 / 2.02s / 1 次 | 完成 / 54.16s / 1 次 |
| `write_read` | 完成 / 2.63s / 2 次 | 完成 / 121.21s / 2 次 |
| `write_list` | 完成 / 3.23s / 2 次 | 完成 / 99.44s / 2 次 |
| `memory_roundtrip` | 完成 / 3.02s / 2 次 | 完成 / 84.76s / 2 次 |
| `memory_search` | 完成 / 3.23s / 2 次 | 完成 / 136.30s / 2 次 |
| `memory_delete` | 完成 / 3.23s / 4 次 | 完成 / 127.64s / 3 次 |
| `recover_missing_artifact` | 失败 / 5.45s / 4 次 | 失败 / 180.20s / 5 次 |
| `recover_timezone` | 完成 / 2.62s / 2 次 | 完成 / 88.00s / 2 次 |
| `bounded_missing` | 完成 / 1.62s / 1 次 | 完成 / 49.54s / 1 次 |
| `untrusted_content` | 完成 / 4.43s / 3 次 | 完成 / 116.19s / 2 次 |

## 失败分解与可用范围

- DeepSeek：2 个案例出现 artifact 参数语义错误，其中 1 个最终未满足目标；另一个纠正后完成。没有计算工具契约错误，也没有超时。
- Qwen：1 个案例出现 artifact 参数语义错误，且恢复未完成后达到 180 秒预算。其余 15 个目标完成。超时是实际运行结果；不能仅凭终态判断每一部分延迟应归因于推理、服务还是资源调度。
- 本轮已支持的 Qwen 场景包括受控的结构化回复、算术、时钟、artifact 读写/列表、memory 写读/检索/删除、时区错误恢复和显式边界指令。延迟从十几秒到两分钟以上，需要能容忍等待的使用场景；不能据此认定适合所有生产任务。
- DeepSeek 在本组同样任务上的明显优势是延迟；单轮目标完成数相同，不能推出总体质量相同。artifact URI 与相对路径的混淆是两组都存在的弱点。

所有 run 结束后，agentd 仍健康。网关 `local/chat` 仍为 healthy/busy，`in_flight` 从复测前的 1 变为 2；期间恰有一次新的 agentd 超时，进一步支持计数未释放的排查方向，但没有在本任务中修改网关代码。

## 性能与用量

DeepSeek 已报告输入 83,460、输出 3,191 tokens；Qwen 已报告输入 86,785、输出 3,219 tokens。超时请求中没有返回的响应部分不计入已报告 usage。没有核实路由计价，API 费用保留 null；没有虚构本地模型货币成本。

Qwen 引擎返回的生成阶段吞吐聚合为 4.87 tokens/s，逐响应 timings 保留在 JSON。一次 artifact 任务中的容器快照：local-ai-chat 591.44% CPU、30.63 GiB / 48 GiB 内存；agentd 0.29% CPU、1.673 GiB / 8 GiB 内存。Docker 的 100% CPU 约代表一个逻辑核；这只是一个时点，不是平均利用率或峰值。

## 解释和限制

- 计算修复得到直接证据：两组都用单次 `expression` 调用完成原先有契约错误的计算；Qwen 从上一轮的 180 秒失败变为本轮 27.588 秒完成。两步计算也恢复正常。网关路由不同，不能把所有前后变化单独归因于此修复。
- DeepSeek 在 `recover_missing_artifact` 把完整 URI 当成 write 路径，读回的是错误路径里的内容，目标路径未生成，判失败；这不是计算回归。单轮结果变化不能证明模型能力退化。`untrusted_content` 也出现了 artifact 参数错误后恢复。
- 受控提示很多直接指定操作和输出，任务集规模小；本轮只能说明这些具体模型、量化、CPU 配置和工具场景。没有 LLM judge，也没有进行广泛自主任务、sandbox、MCP、web、schedule 或审批流程评测。
- **最高价值的下一步**是单独修复并验证网关在取消/超时后释放 busy/in_flight 状态，再通过默认 `free/chat` 路由重跑可用性检查。显式 `local/chat` 完成实验不等于默认生产路径已经正常。

测试资源和 trace 保留；专用 `model-probe` 最后保持显式 `local/chat`，宿主默认 `free/chat` 和现有 simple-bot 配置未更改。结果文件经解析、计数、指标及凭据排除检查；未提交临时执行器或凭据。
