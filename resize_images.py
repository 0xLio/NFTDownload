#!/usr/bin/env python3
"""
NFT图片批量压缩脚本
等比例缩小图片尺寸
"""

from __future__ import annotations

import os
from pathlib import Path
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============== 配置区域 ==============
INPUT_DIR = "nft_images"  # 输入目录
SCALE = 0.5  # 缩放比例 (0.5 = 50%)
OUTPUT_SUFFIX = "_resized"  # 输出目录后缀，设为空字符串则覆盖原文件
QUALITY = 95  # JPEG质量 (1-100)
MAX_WORKERS = 8  # 并发处理线程数
# =====================================

# 支持的图片格式
SUPPORTED_FORMATS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp'}


def resize_image(input_path: Path, output_path: Path, scale: float) -> tuple[bool, str]:
    """缩放单张图片"""
    try:
        with Image.open(input_path) as img:
            # 获取原始尺寸
            original_size = img.size
            
            # 计算新尺寸
            new_width = int(original_size[0] * scale)
            new_height = int(original_size[1] * scale)
            new_size = (new_width, new_height)
            
            # 保持动图的帧（如果是GIF）
            if getattr(img, 'is_animated', False):
                # 处理动图
                frames = []
                durations = []
                
                for frame_num in range(img.n_frames):
                    img.seek(frame_num)
                    frame = img.copy()
                    frame = frame.resize(new_size, Image.Resampling.LANCZOS)
                    frames.append(frame)
                    durations.append(img.info.get('duration', 100))
                
                frames[0].save(
                    output_path,
                    save_all=True,
                    append_images=frames[1:],
                    duration=durations,
                    loop=img.info.get('loop', 0)
                )
            else:
                # 处理静态图片
                resized = img.resize(new_size, Image.Resampling.LANCZOS)
                
                # 保存时保持原格式
                save_kwargs = {}
                if output_path.suffix.lower() in {'.jpg', '.jpeg'}:
                    # JPEG不支持透明通道
                    if resized.mode in ('RGBA', 'LA', 'P'):
                        resized = resized.convert('RGB')
                    save_kwargs['quality'] = QUALITY
                    save_kwargs['optimize'] = True
                elif output_path.suffix.lower() == '.png':
                    save_kwargs['optimize'] = True
                elif output_path.suffix.lower() == '.webp':
                    save_kwargs['quality'] = QUALITY
                
                resized.save(output_path, **save_kwargs)
            
            return True, f"{original_size[0]}x{original_size[1]} -> {new_width}x{new_height}"
    
    except Exception as e:
        return False, str(e)


def resize_image_inline(file_path: Path, scale: float) -> tuple[bool, str]:
    """原地缩放图片，覆盖原文件"""
    file_path = Path(file_path)
    return resize_image(file_path, file_path, scale)


def process_directory(dir_path: Path, scale: float, output_suffix: str):
    """处理单个目录中的所有图片"""
    # 确定输出目录
    if output_suffix:
        output_dir = dir_path.parent / f"{dir_path.name}{output_suffix}"
    else:
        output_dir = dir_path
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 收集所有图片文件
    image_files = []
    for file in dir_path.iterdir():
        if file.is_file() and file.suffix.lower() in SUPPORTED_FORMATS:
            image_files.append(file)
    
    if not image_files:
        print(f"   ⚠️ 目录 {dir_path.name} 中没有找到图片")
        return 0, 0
    
    print(f"\n📁 处理目录: {dir_path.name}")
    print(f"   找到 {len(image_files)} 张图片")
    print(f"   输出目录: {output_dir}")
    
    success_count = 0
    fail_count = 0
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for img_file in image_files:
            output_path = output_dir / img_file.name
            future = executor.submit(resize_image, img_file, output_path, scale)
            futures[future] = img_file.name
        
        for future in as_completed(futures):
            filename = futures[future]
            success, result = future.result()
            
            if success:
                success_count += 1
            else:
                fail_count += 1
                print(f"   ❌ {filename}: {result}")
            
            # 进度显示
            total = success_count + fail_count
            if total % 100 == 0 or total == len(image_files):
                print(f"   进度: {total}/{len(image_files)}")
    
    print(f"   ✅ 完成! 成功: {success_count}, 失败: {fail_count}")
    return success_count, fail_count


def main():
    print("=" * 50)
    print(f"🖼️  NFT图片批量压缩工具")
    print(f"   缩放比例: {SCALE * 100}%")
    print("=" * 50)
    
    input_path = Path(INPUT_DIR)
    
    if not input_path.exists():
        print(f"❌ 输入目录 {INPUT_DIR} 不存在!")
        return
    
    # 获取所有子目录
    subdirs = [d for d in input_path.iterdir() if d.is_dir()]
    
    if not subdirs:
        print(f"⚠️ {INPUT_DIR} 中没有子目录，尝试直接处理该目录...")
        subdirs = [input_path]
    
    print(f"\n找到 {len(subdirs)} 个目录待处理:")
    for d in subdirs:
        print(f"   - {d.name}")
    
    total_success = 0
    total_fail = 0
    
    for subdir in subdirs:
        success, fail = process_directory(subdir, SCALE, OUTPUT_SUFFIX)
        total_success += success
        total_fail += fail
    
    print(f"\n{'='*50}")
    print(f"🏁 全部完成!")
    print(f"   总处理: {total_success + total_fail} 张")
    print(f"   成功: {total_success}")
    print(f"   失败: {total_fail}")
    if OUTPUT_SUFFIX:
        print(f"   压缩后的图片保存在带 '{OUTPUT_SUFFIX}' 后缀的目录中")
    else:
        print(f"   原图片已被覆盖")
    print("=" * 50)


if __name__ == "__main__":
    main()
