#!/usr/bin/env python3
import argparse
import glob
import multiprocessing as mp
import os
import tarfile
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, Iterator, Dict

import polars as pl
import plotext as plt
from tqdm import tqdm


def find_archives(directory: str) -> List[Path]:
    """查找目录下所有的tar.gz和zip压缩包"""
    archives = []
    
    # 查找tar.gz文件
    tar_gz_files = glob.glob(os.path.join(directory, "**/*.tar.gz"), recursive=True)
    archives.extend([Path(f) for f in tar_gz_files])
    
    # 查找zip文件
    zip_files = glob.glob(os.path.join(directory, "**/*.zip"), recursive=True)
    archives.extend([Path(f) for f in zip_files])
    
    return archives


def extract_midi_from_tar(tar_path: Path) -> List[Tuple[str, str, bytes, int]]:
    """从tar.gz文件中抽取MIDI文件"""
    group = tar_path.stem.replace('.tar', '')  # 移除.tar.gz扩展名作为组名
    results = []
    
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.isfile():
                    file_name = member.name
                    # 检查是否是MIDI文件
                    if any(file_name.lower().endswith(ext) for ext in ['.mid', '.midi']):
                        try:
                            file_obj = tar.extractfile(member)
                            if file_obj:
                                content = file_obj.read()
                                file_size = len(content)
                                results.append((group, os.path.basename(file_name), content, file_size))
                        except Exception as e:
                            print(f"警告: 无法从 {tar_path} 中读取 {file_name}: {e}")
    except Exception as e:
        print(f"错误: 无法打开 tar 文件 {tar_path}: {e}")
    
    return results


def extract_midi_from_zip(zip_path: Path) -> List[Tuple[str, str, bytes, int]]:
    """从zip文件中抽取MIDI文件"""
    group = zip_path.stem  # 移除.zip扩展名作为组名
    results = []
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            for file_info in zip_file.infolist():
                if not file_info.is_dir():
                    file_name = file_info.filename
                    # 检查是否是MIDI文件
                    if any(file_name.lower().endswith(ext) for ext in ['.mid', '.midi']):
                        try:
                            content = zip_file.read(file_info)
                            file_size = len(content)
                            results.append((group, os.path.basename(file_name), content, file_size))
                        except Exception as e:
                            print(f"警告: 无法从 {zip_path} 中读取 {file_name}: {e}")
    except Exception as e:
        print(f"错误: 无法打开 zip 文件 {zip_path}: {e}")
    
    return results


def process_single_archive(archive: Path) -> List[Tuple[str, str, bytes, int]]:
    """处理单个压缩包文件 - 用于多进程"""
    if archive.suffix.lower() == '.zip':
        return extract_midi_from_zip(archive)
    elif archive.name.lower().endswith('.tar.gz'):
        return extract_midi_from_tar(archive)
    else:
        return []


def get_size_category(size_bytes: int) -> str:
    """根据文件大小返回分类"""
    if size_bytes < 1024:  # < 1KB
        return "< 1KB"
    elif size_bytes < 10 * 1024:  # < 10KB
        return "1KB - 10KB"
    elif size_bytes < 100 * 1024:  # < 100KB
        return "10KB - 100KB"
    elif size_bytes < 1024 * 1024:  # < 1MB
        return "100KB - 1MB"
    else:
        return "> 1MB"


def plot_size_distribution(df: pl.DataFrame) -> None:
    """绘制MIDI文件大小分布柱状图"""
    # 计算大小分类统计
    size_stats = (df
                  .with_columns(
                      pl.col('file_size').map_elements(get_size_category, return_dtype=pl.Utf8).alias('size_category')
                  )
                  .group_by('size_category')
                  .agg(pl.len().alias('count'))
                  .sort('count', descending=True))
    
    if size_stats.height == 0:
        return
    
    categories = size_stats['size_category'].to_list()
    counts = size_stats['count'].to_list()
    
    print("\n" + "="*60)
    print("MIDI 文件大小分布")
    print("="*60)
    
    # 使用plotext绘制柱状图
    plt.clear_data()
    plt.bar(categories, counts)
    plt.title("MIDI 文件大小分布")
    plt.xlabel("文件大小范围")
    plt.ylabel("文件数量")
    plt.theme('dark')
    plt.plotsize(80, 20)
    plt.show()
    
    # 显示详细统计
    print("\n详细统计:")
    total_files = df.height
    total_size = df['file_size'].sum()
    avg_size = df['file_size'].mean()
    
    print(f"总文件数: {total_files:,}")
    print(f"总大小: {format_size(total_size)}")
    print(f"平均大小: {format_size(int(avg_size))}")
    print(f"最大文件: {format_size(df['file_size'].max())}")
    print(f"最小文件: {format_size(df['file_size'].min())}")
    
    print("\n按大小分类:")
    for row in size_stats.iter_rows(named=True):
        percentage = (row['count'] / total_files) * 100
        print(f"  {row['size_category']:<12}: {row['count']:>6,} 个文件 ({percentage:5.1f}%)")


def format_size(size_bytes: int) -> str:
    """格式化文件大小显示"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def process_archives(archives: List[Path], max_workers: int = None) -> pl.DataFrame:
    """使用多进程处理所有压缩包并返回包含MIDI数据的DataFrame"""
    data = []
    
    if max_workers is None:
        max_workers = min(mp.cpu_count(), len(archives))
    
    print(f"使用 {max_workers} 个进程并行处理 {len(archives)} 个压缩包")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_archive = {executor.submit(process_single_archive, archive): archive 
                           for archive in archives}
        
        # 收集结果，显示进度
        for future in tqdm(as_completed(future_to_archive), total=len(archives), desc="处理压缩包"):
            archive = future_to_archive[future]
            try:
                results = future.result()
                for group, file_name, content, file_size in results:
                    data.append({
                        'group': group,
                        'file_name': file_name,
                        'content': content,
                        'file_size': file_size
                    })
            except Exception as e:
                print(f"处理 {archive} 时出错: {e}")
    
    if not data:
        print("警告: 没有找到任何MIDI文件")
        return pl.DataFrame(schema={
            'group': pl.Utf8,
            'file_name': pl.Utf8,
            'content': pl.Binary,
            'file_size': pl.Int64
        })
    
    return pl.DataFrame(data)


def save_to_parquet(df: pl.DataFrame, output_path: str) -> None:
    """将DataFrame保存为按group分区的parquet文件，开启最高压缩"""
    output_path = Path(output_path)
    
    if df.height == 0:
        print("没有数据可保存")
        return
    
    # 创建输出目录
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"保存数据到 {output_path}")
    
    # 使用最高压缩级别保存，按group分区
    df.write_parquet(
        output_path,
        compression='zstd',  # 使用zstd压缩，通常比gzip更好
        compression_level=22,  # 最高压缩级别
        use_pyarrow=True,
        partition_by='group'
    )
    
    print(f"成功保存 {df.height} 条记录到 {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="从压缩包中提取MIDI文件并保存为Parquet格式",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python main.py ~/data/midiset -o ~/data/midiset.parquet
  python main.py /path/to/archives --output /path/to/output.parquet -j 8
        """
    )
    
    parser.add_argument(
        'input_dir',
        help='包含tar.gz和zip压缩包的输入目录'
    )
    
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='输出parquet文件路径'
    )
    
    parser.add_argument(
        '-j', '--jobs',
        type=int,
        default=None,
        help='并行处理的进程数 (默认: CPU核心数)'
    )
    
    parser.add_argument(
        '--no-plot',
        action='store_true',
        help='不显示大小分布图'
    )
    
    args = parser.parse_args()
    
    # 检查输入目录是否存在
    input_dir = Path(args.input_dir).expanduser().resolve()
    if not input_dir.exists():
        print(f"错误: 输入目录不存在: {input_dir}")
        return 1
    
    if not input_dir.is_dir():
        print(f"错误: {input_dir} 不是一个目录")
        return 1
    
    print(f"搜索目录: {input_dir}")
    
    # 查找所有压缩包
    archives = find_archives(str(input_dir))
    
    if not archives:
        print(f"在 {input_dir} 中没有找到任何 tar.gz 或 zip 文件")
        return 1
    
    print(f"找到 {len(archives)} 个压缩包")
    
    # 处理压缩包
    df = process_archives(archives, max_workers=args.jobs)
    
    if df.height == 0:
        print("没有找到任何MIDI文件")
        return 1
    
    print(f"\n总共找到 {df.height} 个MIDI文件")
    print(f"分组统计:")
    group_counts = df.group_by('group').agg([
        pl.len().alias('count'),
        pl.col('file_size').sum().alias('total_size')
    ]).sort('count', descending=True)
    
    for row in group_counts.iter_rows(named=True):
        print(f"  {row['group']}: {row['count']} 个文件, 总大小: {format_size(row['total_size'])}")
    
    # 显示大小分布图
    if not args.no_plot:
        plot_size_distribution(df)
    
    # 保存到parquet
    output_path = Path(args.output).expanduser().resolve()
    save_to_parquet(df, str(output_path))
    
    return 0


if __name__ == "__main__":
    exit(main())
