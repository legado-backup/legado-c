package io.legado.app.help.ai

import io.legado.app.help.http.newCallResponse
import io.legado.app.help.http.okHttpClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import org.json.JSONObject

/**
 * Local Dream（https://github.com/xororz/local-dream）受控协议转接层。
 *
 * 协议结构：受控模式开启后，主机暴露两个端口——
 * - 控制端口 8808（RemoteHostServer，无鉴权 JSON API）：
 *   GET /info、GET /models、POST /select、GET /status、POST /stop
 * - 生成端口 8081（native 后端）：/generate、/upscale、/tokenize、/health
 *
 * 8081 只有在模型经 /select 拉起后才存在；因此本层负责两件事：
 * 1. 从控制端拉取已安装模型目录（供“从接口列表选择模型”）；
 * 2. 生成前确保后端已按（模型，宽，高）就绪——与 Local Dream 遥控端
 *    checkRemoteBackendHealth 同款判定：running + 模型一致 + 分辨率一致；
 *    拉起后交由生成请求自身与 8081 直接对话。
 *
 * 生成端 baseUrl（供应商配置）形如 http://host:8081/generate，控制端按 host 推导为
 * scheme://host:8808；跨设备时用户只改 baseUrl 里的 IP 即可两端口同步生效。
 */
object AiCreationLocalDream {

    private const val CONTROL_PORT = 8808
    private const val PATH_MODELS = "/models"
    private const val PATH_SELECT = "/select"
    private const val PATH_STATUS = "/status"
    private const val STATE_RUNNING = "running"
    private const val STATE_ERROR = "error"

    // 与 Local Dream 遥控端一致：模型加载需数秒到数分钟，轮询 300ms 起指数退避至 1s，上限 120s
    private const val READY_TIMEOUT_MS = 120_000L
    private const val MIN_POLL_MS = 300L
    private const val MAX_POLL_MS = 1_000L

    /** 已安装模型条目（GET /models 的 models 数组） */
    data class RemoteModel(
        val id: String,
        val name: String
    )

    /** 由生成端 baseUrl 推导控制端基址（scheme://host:8808） */
    fun controlBaseUrlOf(generationBaseUrl: String): String {
        val url = generationBaseUrl.trim().toHttpUrlOrNull()
            ?: throw IllegalStateException("Local Dream 生成地址无效：$generationBaseUrl")
        return "${url.scheme}://${url.host}:$CONTROL_PORT"
    }

    /** 拉取主机已安装模型目录（id 为唯一键，name 供展示） */
    suspend fun fetchModels(provider: AiCreationProviderConfig): List<RemoteModel> =
        withContext(Dispatchers.IO) {
            val base = controlBaseUrlOf(provider.baseUrl)
            val payload = getJson(base, PATH_MODELS)
            val array = payload.optJSONArray("models") ?: return@withContext emptyList()
            (0 until array.length()).mapNotNull { index ->
                val item = array.optJSONObject(index) ?: return@mapNotNull null
                val id = item.optString("id").trim()
                if (id.isEmpty()) return@mapNotNull null
                RemoteModel(
                    id = id,
                    name = item.optString("name", id).ifBlank { id }
                )
            }
        }

    /**
     * 生成前确保主机后端已按（模型，宽，高）就绪：
     * 状态不满足时 POST /select 拉起并轮询至 running；error 态原样抛出主机消息。
     * [onStatus] 用于过程提示（正在拉起/加载模型）。
     */
    suspend fun ensureBackendRunning(
        provider: AiCreationProviderConfig,
        modelId: String,
        width: Int,
        height: Int,
        onStatus: (String) -> Unit = {}
    ) = withContext(Dispatchers.IO) {
        val base = controlBaseUrlOf(provider.baseUrl)

        fun ready(status: JSONObject): Boolean {
            if (status.optString("state") != STATE_RUNNING) return false
            return status.optString("serving_model_id") == modelId &&
                status.optInt("width") == width &&
                status.optInt("height") == height
        }

        var status = runCatching { getJson(base, PATH_STATUS) }.getOrNull()
        if (status == null) {
            throw IllegalStateException(
                "无法连接 Local Dream 控制端口（$base）：" +
                    "请确认已在其「设备互联」中开启受控模式"
            )
        }
        if (!ready(status)) {
            onStatus("Local Dream 正在启动模型（$modelId）…")
            val selectPayload = JSONObject().apply {
                put("model_id", modelId)
                put("width", width)
                put("height", height)
            }
            val response = try {
                postControl(base, PATH_SELECT, selectPayload)
            } catch (throwable: Throwable) {
                if (throwable is kotlinx.coroutines.CancellationException) throw throwable
                throw IllegalStateException(
                    "Local Dream 拉起模型失败（$modelId）：${throwable.message}",
                    throwable
                )
            }
            if (response == null) {
                throw IllegalStateException(
                    "Local Dream 拉起模型失败（$modelId）：主机响应无法解析"
                )
            }
            val start = System.currentTimeMillis()
            var pollDelay = MIN_POLL_MS
            while (true) {
                if (System.currentTimeMillis() - start > READY_TIMEOUT_MS) {
                    throw IllegalStateException("Local Dream 模型加载超时（${READY_TIMEOUT_MS / 1000} 秒）")
                }
                delay(pollDelay)
                pollDelay = (pollDelay * 2).coerceAtMost(MAX_POLL_MS)
                status = runCatching { getJson(base, PATH_STATUS) }.getOrNull()
                    ?: continue
                val state = status.optString("state")
                if (state == STATE_ERROR) {
                    val message = status.optString("message")
                        .ifBlank { "模型启动失败（无主机消息）" }
                    throw IllegalStateException("Local Dream 模型启动失败：$message")
                }
                if (ready(status)) break
            }
        }
    }

    private suspend fun getJson(base: String, path: String): JSONObject {
        val response = okHttpClient.newCallResponse {
            url(base.trimEnd('/') + path)
            addHeader("Accept", "application/json")
        }
        response.use { raw ->
            val payload = raw.body?.string().orEmpty()
            if (!raw.isSuccessful) {
                throw IllegalStateException("Local Dream 控制请求失败 HTTP ${raw.code}: ${payload.take(200)}")
            }
            return JSONObject(payload)
        }
    }

    /** 控制端 POST：HTTP 失败原样抛错，非 2xx 视为业务失败；返回体解析失败返回 null */
    private suspend fun postControl(base: String, path: String, body: JSONObject): JSONObject? {
        val response = okHttpClient.newCallResponse {
            url(base.trimEnd('/') + path)
            addHeader("Accept", "application/json")
            addHeader("Content-Type", "application/json")
            postJson(body.toString())
        }
        response.use { raw ->
            val payload = raw.body?.string().orEmpty()
            if (!raw.isSuccessful) {
                throw IllegalStateException(
                    "Local Dream 控制请求失败 HTTP ${raw.code}: ${payload.take(200)}"
                )
            }
            return runCatching { JSONObject(payload) }.getOrNull()
        }
    }
}
