# MIDI Parquet 工具

一个用于从压缩包中提取MIDI文件并保存为Parquet格式的命令行工具。

## 功能特性

- 支持从 `tar.gz` 和 `zip` 压缩包中提取MIDI文件
- 支持所有MIDI文件扩展名：`.mid`, `.MIDI`, `.MID`, `.midi`
- 在内存中完成解压，不写入临时文件到磁盘
- 按压缩包名称分组，使用Parquet分区存储
- 开启最高等级压缩（zstd level 22）
- 显示进度条和统计信息

## 安装依赖

```bash
uv sync
```

## 使用方法

```bash
# 基本用法
uv run python main.py ~/data/midiset -o ~/data/midiset.parquet

# 或者使用完整路径
uv run python main.py /path/to/archives --output /path/to/output.parquet
```

## 输出格式

生成的Parquet文件包含以下字段：
- `group` (string): 压缩包的名称（不含扩展名）
- `file_name` (string): MIDI文件的原始文件名
- `content` (binary): MIDI文件的二进制内容

文件按`group`字段分区存储，便于后续按组查询和处理。

## 示例

假设你有如下目录结构：
```
~/data/midiset/
├── classical_music.tar.gz
├── jazz_collection.zip
└── pop_songs.tar.gz
```

运行命令：
```bash
uv run python main.py ~/data/midiset -o ~/data/midiset.parquet
```

将生成分区的Parquet文件，包含所有MIDI文件的内容和元数据。