#!/usr/bin/env python3
"""
NFT Collection Image Downloader
使用Alchemy API下载NFT合集的所有图片
"""

import os
import json
import asyncio
import aiohttp
import aiofiles
import requests
from pathlib import Path
from typing import Optional, Callable
from urllib.parse import urlparse
import time
import urllib3
from io import BytesIO

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 默认设置
CONCURRENT_DOWNLOADS = 50
PAGE_SIZE = 100
REQUEST_DELAY = 0.5
MAX_RETRIES = 3


def convert_ipfs_url(url: str) -> str:
    """将IPFS URL转换为HTTP网关URL"""
    if not url:
        return url
    
    # 处理 ipfs:// 协议
    if url.startswith("ipfs://"):
        hash_part = url[7:]  # 移除 "ipfs://"
        return f"https://ipfs.io/ipfs/{hash_part}"
    
    # 处理 ipfs/Qm... 格式
    if "ipfs/" in url and not url.startswith("http"):
        return f"https://ipfs.io/{url}"
    
    return url


def get_image_url_from_metadata(nft: dict) -> Optional[str]:
    """从NFT元数据中提取图片URL"""
    # 优先使用 media 字段（Alchemy处理过的）
    media = nft.get("media", [])
    if media and len(media) > 0:
        gateway_url = media[0].get("gateway")
        if gateway_url:
            return gateway_url
        raw_url = media[0].get("raw")
        if raw_url:
            return convert_ipfs_url(raw_url)
    
    # 从 metadata 中获取
    metadata = nft.get("metadata", {}) or {}
    
    # 尝试常见的图片字段
    for field in ["image", "image_url", "image_data", "animation_url"]:
        if field in metadata and metadata[field]:
            return convert_ipfs_url(metadata[field])
    
    # 从 rawMetadata 获取
    raw_metadata = nft.get("rawMetadata", {}) or {}
    for field in ["image", "image_url"]:
        if field in raw_metadata and raw_metadata[field]:
            return convert_ipfs_url(raw_metadata[field])
    
    return None


def get_nft_name(nft: dict) -> str:
    """获取NFT名称或ID"""
    token_id = nft.get("id", {}).get("tokenId", "unknown")
    # 转换十六进制tokenId为十进制
    try:
        if token_id.startswith("0x"):
            token_id = str(int(token_id, 16))
    except:
        pass
    
    metadata = nft.get("metadata", {}) or {}
    name = metadata.get("name", f"token_{token_id}")
    
    # 清理文件名中的非法字符
    name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).strip()
    if not name:
        name = f"token_{token_id}"
    
    return name


def fetch_with_retry(url: str, params: dict, max_retries: int = MAX_RETRIES, log_callback: Callable = print):
    """带重试的HTTP请求"""
    for attempt in range(max_retries):
        try:
            # 创建session并配置重试
            session = requests.Session()
            adapter = requests.adapters.HTTPAdapter(
                max_retries=requests.adapters.Retry(
                    total=3,
                    backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504]
                )
            )
            session.mount("https://", adapter)

            response = session.get(url, params=params, timeout=60, verify=True)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.SSLError as e:
            log_callback(f"   ⚠️ SSL错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                log_callback(f"   等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            log_callback(f"   ⚠️ 请求错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                log_callback(f"   等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
    return None


def fetch_collection_nfts(contract_address: str, api_key: str,
                          log_callback: Callable = print, limit: int = 0,
                          cancel_check: Callable = None):
    """获取合集中所有NFT的元数据"""
    base_url = f"https://eth-mainnet.g.alchemy.com/v2/{api_key}"
    log_callback(f"📦 正在获取合约 {contract_address} 的NFT列表...")

    all_nfts = []
    start_token = None
    page = 1
    consecutive_errors = 0
    max_consecutive_errors = 5

    while True:
        if cancel_check and cancel_check():
            log_callback("⏹ 用户取消，停止获取元数据")
            break

        url = f"{base_url}/getNFTsForCollection"
        params = {
            "contractAddress": contract_address,
            "withMetadata": "true",
            "limit": PAGE_SIZE,
        }
        if start_token:
            params["startToken"] = start_token

        data = fetch_with_retry(url, params, log_callback=log_callback)

        if data is None:
            consecutive_errors += 1
            log_callback(f"❌ API请求失败，连续错误次数: {consecutive_errors}")
            if consecutive_errors >= max_consecutive_errors:
                log_callback(f"❌ 连续错误过多，停止获取")
                break
            time.sleep(5)
            continue

        consecutive_errors = 0
        nfts = data.get("nfts", [])
        all_nfts.extend(nfts)

        log_callback(f"   第 {page} 页: 获取 {len(nfts)} 个NFT, 总计: {len(all_nfts)}")

        # 检查数量限制
        if limit > 0 and len(all_nfts) >= limit:
            all_nfts = all_nfts[:limit]
            log_callback(f"   已达到数量限制 {limit}，停止获取")
            break

        # 检查是否有下一页
        next_token = data.get("nextToken")
        if not next_token or len(nfts) == 0:
            break

        start_token = next_token
        page += 1
        time.sleep(REQUEST_DELAY)

    log_callback(f"✅ 合约 {contract_address[:10]}... 共找到 {len(all_nfts)} 个NFT")
    return all_nfts


async def download_image(session: aiohttp.ClientSession, url: str, save_path: Path,
                         semaphore: asyncio.Semaphore, scale: float = None,
                         timeout: int = 60, max_retries: int = 3):
    """异步下载单张图片，可选缩放，支持重试"""
    async with semaphore:
        last_error = ""
        for attempt in range(max_retries):
            try:
                url = convert_ipfs_url(url)

                headers = {
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
                }

                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    if response.status == 200:
                        content = await response.read()

                        # 确定文件扩展名
                        content_type = response.headers.get("content-type", "")
                        ext = ".png"
                        if "jpeg" in content_type or "jpg" in content_type:
                            ext = ".jpg"
                        elif "gif" in content_type:
                            ext = ".gif"
                        elif "svg" in content_type:
                            ext = ".svg"
                        elif "webp" in content_type:
                            ext = ".webp"

                        if not save_path.suffix:
                            save_path = save_path.with_suffix(ext)

                        # 如果需要缩放且不是 SVG
                        if scale and scale != 1.0 and ext != ".svg":
                            try:
                                from resize_images import resize_image_inline
                                async with aiofiles.open(save_path, 'wb') as f:
                                    await f.write(content)
                                await asyncio.to_thread(resize_image_inline, save_path, scale)
                            except Exception:
                                async with aiofiles.open(save_path, 'wb') as f:
                                    await f.write(content)
                        else:
                            async with aiofiles.open(save_path, 'wb') as f:
                                await f.write(content)

                        return True, str(save_path)
                    elif response.status == 429:
                        # 被限流，等待后重试
                        wait = (attempt + 1) * 2
                        await asyncio.sleep(wait)
                        last_error = f"HTTP 429 (限流)"
                        continue
                    else:
                        last_error = f"HTTP {response.status}"
                        if attempt < max_retries - 1:
                            await asyncio.sleep(1)
                            continue
            except asyncio.TimeoutError:
                last_error = "超时"
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
            except Exception as e:
                last_error = str(e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue

        return False, last_error


async def download_collection_images(contract_address: str, nfts: list,
                                      output_dir: str = "nft_images",
                                      log_callback: Callable = print,
                                      progress_callback: Callable = None,
                                      scale: float = None,
                                      cancel_check: Callable = None,
                                      concurrent: int = CONCURRENT_DOWNLOADS,
                                      timeout: int = 60,
                                      max_retries: int = 3):
    """异步下载合集的所有图片"""
    short_address = contract_address[:10]
    save_dir = Path(output_dir) / short_address
    save_dir.mkdir(parents=True, exist_ok=True)

    download_tasks = []
    for nft in nfts:
        image_url = get_image_url_from_metadata(nft)
        if not image_url:
            continue
        nft_name = get_nft_name(nft)
        save_path = save_dir / nft_name
        download_tasks.append((image_url, save_path))

    log_callback(f"🚀 开始下载 {len(download_tasks)} 张图片到 {save_dir}/ (并发:{concurrent}, 超时:{timeout}s, 重试:{max_retries})")

    semaphore = asyncio.Semaphore(concurrent)
    success_count = 0
    fail_count = 0

    connector = aiohttp.TCPConnector(limit=concurrent, force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            download_image(session, url, path, semaphore, scale=scale,
                          timeout=timeout, max_retries=max_retries)
            for url, path in download_tasks
        ]

        for i, task in enumerate(asyncio.as_completed(tasks)):
            if cancel_check and cancel_check():
                log_callback("⏹ 用户取消，停止下载")
                break

            success, result = await task
            if success:
                success_count += 1
            else:
                fail_count += 1

            total = success_count + fail_count
            if progress_callback:
                progress_callback(total, len(tasks))
            if total % 50 == 0 or total == len(tasks):
                log_callback(f"   进度: {total}/{len(tasks)} (成功: {success_count}, 失败: {fail_count})")

    log_callback(f"✅ 下载完成! 成功: {success_count}, 失败: {fail_count}")
    return success_count, fail_count


def main():
    """CLI 入口"""
    print("=" * 50)
    print("🎨 NFT Collection Image Downloader")
    print("=" * 50)

    api_key = input("请输入 Alchemy API Key: ").strip()
    if not api_key:
        print("❌ API Key 不能为空")
        return

    contracts_input = input("请输入合约地址 (多个用逗号分隔): ").strip()
    contracts = [c.strip() for c in contracts_input.split(",") if c.strip()]
    if not contracts:
        print("❌ 至少需要一个合约地址")
        return

    limit_input = input("每个合集下载数量限制 (0=全部): ").strip()
    limit = int(limit_input) if limit_input else 0

    scale_input = input("缩放比例 (1.0=不缩放, 0.5=50%): ").strip()
    scale = float(scale_input) if scale_input else 1.0

    output_dir = input("输出目录 (默认 nft_images): ").strip() or "nft_images"

    total_success = 0
    total_fail = 0

    for contract in contracts:
        print(f"\n{'='*50}")
        print(f"处理合约: {contract}")
        print("=" * 50)

        nfts = fetch_collection_nfts(contract, api_key, limit=limit)

        if not nfts:
            print(f"⚠️ 合约 {contract} 没有找到NFT")
            continue

        success, fail = asyncio.run(
            download_collection_images(contract, nfts, output_dir=output_dir, scale=scale)
        )
        total_success += success
        total_fail += fail

    print(f"\n{'='*50}")
    print(f"🏁 全部完成!")
    print(f"   总成功: {total_success}")
    print(f"   总失败: {total_fail}")
    print(f"   图片保存在: {output_dir}/")
    print("=" * 50)


if __name__ == "__main__":
    main()
