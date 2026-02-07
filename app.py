#!/usr/bin/env python3
"""NFT Batch Downloader - Web 本地应用"""

import asyncio
import json
import os
import sys
import webbrowser
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from nft_downloader import fetch_collection_nfts, download_collection_images

# 确定模板目录（兼容 PyInstaller 打包）
if getattr(sys, 'frozen', False):
    BASE_DIR = Path(sys._MEIPASS)
else:
    BASE_DIR = Path(__file__).parent

app = FastAPI(title="NFT Batch Downloader")

# 全局状态
class AppState:
    def __init__(self):
        self.is_running = False
        self.cancel_flag = False
        self.event_queue: Optional[asyncio.Queue] = None
        self.current_task: Optional[asyncio.Task] = None

state = AppState()


def send_event(event_type: str, data: dict):
    """向事件队列发送事件"""
    if state.event_queue:
        try:
            state.event_queue.put_nowait({"event": event_type, "data": data})
        except asyncio.QueueFull:
            pass


def log_callback(message: str):
    """日志回调，推送到 SSE"""
    send_event("log", {"message": message})


def progress_callback(current: int, total: int):
    """进度回调，推送到 SSE"""
    send_event("progress", {"current": current, "total": total})


def cancel_check() -> bool:
    """检查是否取消"""
    return state.cancel_flag


@app.get("/", response_class=HTMLResponse)
async def index():
    """返回前端页面"""
    html_path = BASE_DIR / "templates" / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.post("/api/start")
async def start_download(request: Request):
    """开始下载任务"""
    if state.is_running:
        return JSONResponse({"error": "任务正在运行中"}, status_code=400)

    body = await request.json()
    api_key = body.get("api_key", "").strip()
    contracts = body.get("contracts", [])
    scale = float(body.get("scale", 1.0))
    limit = int(body.get("limit", 0))
    output_dir = body.get("output_dir", "nft_images").strip()
    concurrent = int(body.get("concurrent", 10))
    timeout = int(body.get("timeout", 120))
    max_retries = int(body.get("max_retries", 3))

    if not api_key:
        return JSONResponse({"error": "请输入 Alchemy API Key"}, status_code=400)
    if not contracts:
        return JSONResponse({"error": "请至少添加一个合约地址"}, status_code=400)

    state.is_running = True
    state.cancel_flag = False
    state.event_queue = asyncio.Queue(maxsize=1000)

    state.current_task = asyncio.create_task(
        run_download(api_key, contracts, scale, limit, output_dir,
                     concurrent, timeout, max_retries)
    )

    return {"status": "started"}


@app.post("/api/stop")
async def stop_download():
    """停止下载"""
    if not state.is_running:
        return JSONResponse({"error": "没有正在运行的任务"}, status_code=400)
    state.cancel_flag = True
    send_event("log", {"message": "⏹ 正在停止..."})
    return {"status": "stopping"}


@app.get("/api/status")
async def status_stream():
    """SSE 事件流"""
    async def event_generator():
        state.event_queue = state.event_queue or asyncio.Queue(maxsize=1000)
        while True:
            try:
                event = await asyncio.wait_for(state.event_queue.get(), timeout=30)
                event_type = event["event"]
                data = json.dumps(event["data"], ensure_ascii=False)
                yield f"event: {event_type}\ndata: {data}\n\n"
            except asyncio.TimeoutError:
                yield f"event: ping\ndata: {{}}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def run_download(api_key: str, contracts: list, scale: float,
                       limit: int, output_dir: str,
                       concurrent: int = 10, timeout: int = 120,
                       max_retries: int = 3):
    """后台下载任务"""
    total_success = 0
    total_fail = 0

    try:
        for i, contract in enumerate(contracts):
            if state.cancel_flag:
                break

            send_event("log", {"message": f"━━━ 合集 {i+1}/{len(contracts)}: {contract} ━━━"})
            send_event("collection", {
                "index": i + 1,
                "total": len(contracts),
                "contract": contract,
            })

            # 获取 NFT 元数据（同步函数，放到线程中）
            nfts = await asyncio.to_thread(
                fetch_collection_nfts,
                contract, api_key,
                log_callback=log_callback,
                limit=limit,
                cancel_check=cancel_check,
            )

            if not nfts or state.cancel_flag:
                if not nfts:
                    send_event("log", {"message": f"⚠️ 合约 {contract} 没有找到NFT"})
                continue

            # 下载图片
            effective_scale = scale if scale and scale != 1.0 else None
            success, fail = await download_collection_images(
                contract, nfts,
                output_dir=output_dir,
                log_callback=log_callback,
                progress_callback=progress_callback,
                scale=effective_scale,
                cancel_check=cancel_check,
                concurrent=concurrent,
                timeout=timeout,
                max_retries=max_retries,
            )
            total_success += success
            total_fail += fail

        send_event("done", {
            "success": total_success,
            "fail": total_fail,
            "cancelled": state.cancel_flag,
        })
    except Exception as e:
        send_event("error_event", {"message": str(e)})
    finally:
        state.is_running = False
        state.cancel_flag = False


def open_browser():
    """延迟打开浏览器"""
    import threading
    import time

    def _open():
        time.sleep(1.5)
        webbrowser.open("http://localhost:8888")

    threading.Thread(target=_open, daemon=True).start()


if __name__ == "__main__":
    print("🚀 NFT Batch Downloader 启动中...")
    print("   浏览器将自动打开 http://localhost:8888")
    print("   按 Ctrl+C 退出\n")
    open_browser()
    uvicorn.run(app, host="0.0.0.0", port=8888, log_level="warning")
