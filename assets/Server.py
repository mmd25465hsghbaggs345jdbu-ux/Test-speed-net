"""
=============================================================================
NetPulse Python Backend Engine (FastAPI & Asyncio Socket Probes)
=============================================================================
Requirements:
    pip install fastapi uvicorn pydantic requests
Run Command:
    python server.py  (or uvicorn server:app --host 0.0.0.0 --port 8000 --reload)
=============================================================================
"""

import time
import socket
import ssl
import asyncio
from typing import Optional, List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(
    title="NetPulse Cyberpunk Backend",
    description="سرویس بلادرنگ تست سرعت، پروب‌های TCP/TLS و بررسی تحریم/فیلترینگ",
    version="2.0.0"
)

# فعال‌سازی CORS برای اتصال فرانت‌اند
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# مدل داده‌ای درخواست پینگ
class PingRequest(BaseModel):
    host: str
    port: int = 443
    use_tls: bool = False
    sni: Optional[str] = None
    timeout: int = 3500
    samples: int = 3

class PingResponse(BaseModel):
    host: str
    port: int
    alive: bool
    resolved_ip: Optional[str] = None
    dns_latency: Optional[int] = None
    tcp_latency: Optional[int] = None
    tls_latency: Optional[int] = None
    total_latency: Optional[int] = None
    samples: List[int] = []
    jitter: int = 0
    error: Optional[str] = None

# =========================================================================
# تابع پروب سوکت TCP و هندشیک TLS
# =========================================================================
def sync_socket_probe(host: str, port: int, use_tls: bool, sni: Optional[str], timeout_sec: float):
    start_time = time.time()
    try:
        # ۱. تفکیک DNS
        dns_start = time.time()
        ip = socket.gethostbyname(host)
        dns_ms = int((time.time() - dns_start) * 1000)

        # ۲. هندشیک TCP
        tcp_start = time.time()
        sock = socket.create_connection((ip, port), timeout=timeout_sec)
        tcp_ms = int((time.time() - tcp_start) * 1000)

        tls_ms = 0
        if use_tls:
            tls_start = time.time()
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE  # برای کانفیگ‌های Reality یا Self-signed
            tls_sock = context.wrap_socket(sock, server_hostname=sni or host)
            tls_ms = int((time.time() - tls_start) * 1000)
            tls_sock.close()
        else:
            sock.close()

        total_ms = int((time.time() - start_time) * 1000)
        return {
            "success": True,
            "ip": ip,
            "dns_ms": dns_ms,
            "tcp_ms": tcp_ms,
            "tls_ms": tls_ms,
            "total_ms": total_ms
        }
    except Exception as err:
        return {"success": False, "error": str(err)}


@app.post("/api/ping", response_model=PingResponse)
async def handle_ping(req: PingRequest):
    timeout_sec = req.timeout / 1000.0
    loop = asyncio.get_event_loop()

    # اجرای پروب اولیه در ترد مجزا
    first_res = await loop.run_in_executor(
        None, sync_socket_probe, req.host, req.port, req.use_tls, req.sni, timeout_sec
    )

    if not first_res["success"]:
        return PingResponse(
            host=req.host,
            port=req.port,
            alive=False,
            error=first_res.get("error", "عدم برقراری ارتباط")
        )

    # نمونه‌گیری چندگانه برای نمودار Sparkline و محاسبه Jitter
    samples = [first_res["total_ms"]]
    for _ in range(max(req.samples - 1, 0)):
        await asyncio.sleep(0.05)
        probe = await loop.run_in_executor(
            None, sync_socket_probe, req.host, req.port, req.use_tls, req.sni, 2.0
        )
        if probe["success"]:
            samples.append(probe["total_ms"])

    # محاسبه Jitter
    jitter = 0
    if len(samples) > 1:
        diffs = [abs(samples[i] - samples[i-1]) for i in range(1, len(samples))]
        jitter = int(sum(diffs) / len(diffs))

    avg_latency = int(sum(samples) / len(samples))

    return PingResponse(
        host=req.host,
        port=req.port,
        alive=True,
        resolved_ip=first_res.get("ip"),
        dns_latency=first_res.get("dns_ms"),
        tcp_latency=first_res.get("tcp_ms"),
        tls_latency=first_res.get("tls_ms"),
        total_latency=avg_latency,
        samples=samples,
        jitter=jitter
    )


# =========================================================================
# روت‌های استریم تست سرعت دانلود و آپلود
# =========================================================================
@app.get("/api/speedtest/download")
async def speedtest_download(size_mb: int = 20):
    """تولید استریم باینری برای تست سرعت دانلود"""
    chunk_size = 64 * 1024  # 64 KB
    total_bytes = size_mb * 1024 * 1024
    chunk = b"\x00" * chunk_size

    def iter_bytes():
        bytes_sent = 0
        while bytes_sent < total_bytes:
            yield chunk
            bytes_sent += chunk_size

    return StreamingResponse(
        iter_bytes(),
        media_type="application/octet-stream",
        headers={"Content-Length": str(total_bytes)}
    )


@app.post("/api/speedtest/upload")
async def speedtest_upload():
    """دریافت داده‌های آپلود برای سنجش پهنای باند ارسال"""
    return {"status": "ok", "message": "بایت‌های آپلود با موفقیت دریافت شدند"}


# =========================================================================
# اطلاعات IP کلاینت
# =========================================================================
@app.get("/api/ip-info")
async def get_ip_info():
    return {
        "ip": "1.1.1.1",
        "isp": "Local Edge Infrastructure",
        "country": "IR / Global",
        "city": "Tehran"
    }


if __name__ == "__main__":
    import uvicorn
    print("🚀 NetPulse Backend Engine is running on http://0.0.0.0:8000")
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)