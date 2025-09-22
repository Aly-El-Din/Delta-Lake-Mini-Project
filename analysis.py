import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style for better plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

MEMORY_DIR = "C:\\Users\\Cyber\\Downloads\\memory_6" 
OUTPUT_FILE = "analysis_report.txt"


def extract_thread_info_from_filename(filename):
    """Extract thread info from filename if available"""
    try:
        base_name = Path(filename).stem
        if 'test_table' in base_name:
            thread_id = base_name.replace('test_table', '')
            return f"Thread-{thread_id}" if thread_id else "Unknown"
    except:
        pass
    return Path(filename).stem


def write_to_log(text, mode="a"):
    """Helper to write logs to file and also print"""
    with open(OUTPUT_FILE, mode, encoding="utf-8") as f:
        f.write(text + "\n")


def load_csv_files(directory):
    """Load CSV files and extract thread performance data"""
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) 
                 if f.endswith(".csv") and not f.startswith('output_')]
    
    file_data = []
    thread_times = []
    
    write_to_log(f"Found {len(csv_files)} CSV files to analyze\n", mode="w")
    
    for file_path in csv_files:
        try:
            filename = os.path.basename(file_path)
            write_to_log(f"Processing: {filename}")
            
            df = pd.read_csv(file_path)
            num_rows = len(df)
            
            if num_rows == 0:
                write_to_log("Empty file, skipping\n")
                continue
            
            # Extract metadata
            thread_name, elapsed_time, data_rows = None, None, num_rows
            if num_rows > 0:
                try:
                    last_row = df.iloc[-1]
                    if len(df.columns) >= 2:
                        potential_thread = str(last_row.iloc[0])
                        potential_time = str(last_row.iloc[1])
                        
                        if 'thread' in potential_thread.lower():
                            try:
                                elapsed_time = float(potential_time)
                                thread_name = potential_thread
                                data_rows = num_rows - 1
                            except ValueError:
                                pass
                    
                    if elapsed_time is None:
                        for col_val in last_row:
                            try:
                                val = float(col_val)
                                if 1000 <= val <= 10000:
                                    elapsed_time = val
                                    thread_name = extract_thread_info_from_filename(filename)
                                    data_rows = num_rows - 1
                                    break
                            except (ValueError, TypeError):
                                continue
                except Exception as e:
                    write_to_log(f"Error extracting metadata: {e}")
            
            if thread_name is None:
                thread_name = extract_thread_info_from_filename(filename)
                elapsed_time = data_rows * 0.1
            
            file_size = os.path.getsize(file_path)
            actual_data = df.iloc[:data_rows] if data_rows < num_rows else df
            
            # compute avg col size (important for new plot)
            avg_col_sizes = actual_data.memory_usage(deep=True) / len(actual_data) if len(actual_data) > 0 else None
            avg_col_size = avg_col_sizes.mean() if avg_col_sizes is not None else 0
            
            file_info = {
                'filename': filename,
                'thread_name': thread_name,
                'elapsed_time': elapsed_time,
                'file_size_bytes': file_size,
                'file_size_mb': file_size / (1024 * 1024),
                'total_rows': num_rows,
                'data_rows': data_rows,
                'num_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
                'avg_col_size_bytes': avg_col_size
            }
            
            file_data.append(file_info)
            thread_times.append((thread_name, elapsed_time))
            
            write_to_log(
                f"Thread: {thread_name}\n"
                f"Time: {elapsed_time:.1f}ms\n"
                f"Rows: {data_rows}, Columns: {len(df.columns)}\n"
                f"Size: {file_size/1024:.1f}KB\n"
            )
            
        except Exception as e:
            write_to_log(f"Error processing {filename}: {e}\n")
            continue
    
    return file_data, thread_times


def analyze_thread_performance(file_data, thread_times):
    df_files = pd.DataFrame(file_data)
    df_times = pd.DataFrame(thread_times, columns=["thread_name", "elapsed_time"])
    
    if df_files.empty or df_times.empty:
        write_to_log("No valid data found for analysis")
        return None, None
    
    df_combined = df_files.merge(df_times, on='thread_name', how='outer', suffixes=('', '_duplicate'))
    
    write_to_log("\n" + "="*60)
    write_to_log("THREAD PERFORMANCE ANALYSIS")
    write_to_log("="*60)
    
    write_to_log("\nBASIC STATISTICS:")
    write_to_log(f"Total threads analyzed: {len(df_times)}")
    write_to_log(f"Average execution time: {df_times['elapsed_time'].mean():.1f}ms")
    write_to_log(f"Median execution time: {df_times['elapsed_time'].median():.1f}ms")
    write_to_log(f"Fastest thread: {df_times['elapsed_time'].min():.1f}ms")
    write_to_log(f"Slowest thread: {df_times['elapsed_time'].max():.1f}ms")
    write_to_log(f"Time variance: {df_times['elapsed_time'].var():.1f}")
    write_to_log(f"Standard deviation: {df_times['elapsed_time'].std():.1f}ms")
    
    for p in [25, 50, 75, 90, 95]:
        val = np.percentile(df_times['elapsed_time'], p)
        write_to_log(f"{p}th percentile: {val:.1f}ms")
    
    return df_combined, df_files.corrwith(df_times.set_index('thread_name')['elapsed_time'], method='pearson')


def create_visualizations(df_combined):
    """Create selected visualizations (time distribution + col size vs execution time)"""
    if df_combined is None or df_combined.empty:
        print("No data available for visualization")
        return
    
    plt.figure(figsize=(18, 10))

    # 1. Execution time distribution
    plt.subplot(1, 2, 1)
    plt.hist(df_combined['elapsed_time'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
    plt.axvline(df_combined['elapsed_time'].mean(), color='red', linestyle='--', label=f'Mean: {df_combined["elapsed_time"].mean():.1f}ms')
    plt.axvline(df_combined['elapsed_time'].median(), color='green', linestyle='--', label=f'Median: {df_combined["elapsed_time"].median():.1f}ms')
    plt.xlabel('Execution Time (ms)')
    plt.ylabel('Frequency')
    plt.title('Thread Execution Time Distribution')
    plt.legend()
    plt.grid(True, alpha=0.3)

    # 2. Avg col size vs execution time
    plt.subplot(1, 2, 2)
    sns.scatterplot(x="avg_col_size_bytes", y="elapsed_time", data=df_combined, hue="thread_name", s=80)
    plt.xlabel("Average Column Size (bytes)")
    plt.ylabel("Execution Time (ms)")
    plt.title("Avg Column Size vs Thread Execution Time")
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()


def identify_performance_issues(df_combined, correlation_data):
    """Identify potential performance bottlenecks"""
    print("\n" + "="*60)
    print("PERFORMANCE BOTTLENECK ANALYSIS")
    print("="*60)
    
    if df_combined is None or df_combined.empty:
        print("No data available for bottleneck analysis")
        return
    
    mean_time = df_combined['elapsed_time'].mean()
    std_time = df_combined['elapsed_time'].std()
    threshold = mean_time + 2 * std_time
    
    outliers = df_combined[df_combined['elapsed_time'] > threshold]
    
    print(f"\nPERFORMANCE OUTLIERS (>{threshold:.1f}ms):")
    if len(outliers) > 0:
        for _, row in outliers.iterrows():
            print(f"Thread: {row['thread_name']}")
            print(f"Time: {row['elapsed_time']:.1f}ms ({((row['elapsed_time']/mean_time-1)*100):+.1f}% vs avg)")
            print(f"File size: {row['file_size_mb']:.2f}MB")
            print(f"Rows: {row['data_rows']:,}")
            print(f"Memory: {row['memory_usage_mb']:.2f}MB")
            print(f"Avg Col Size: {row['avg_col_size_bytes']:.1f} bytes\n")
    else:
        print("No significant outliers detected")
    
    print("\nRESOURCE UTILIZATION PATTERNS:")
    print(f"File size range: {df_combined['file_size_mb'].min():.2f}MB - {df_combined['file_size_mb'].max():.2f}MB")
    print(f"Row count range: {df_combined['data_rows'].min():,} - {df_combined['data_rows'].max():,}")
    print(f"Memory usage range: {df_combined['memory_usage_mb'].min():.2f}MB - {df_combined['memory_usage_mb'].max():.2f}MB")
    print(f"Avg col size range: {df_combined['avg_col_size_bytes'].min():.1f} - {df_combined['avg_col_size_bytes'].max():.1f} bytes")
    
    print("\nOPTIMIZATION RECOMMENDATIONS:")
    if 'avg_col_size_bytes' in correlation_data and abs(correlation_data['avg_col_size_bytes']) > 0.5:
        print("Average column size strongly correlates with execution time")
        print("→ Large string/complex columns may slow down performance")
    
    if 'file_size_mb' in correlation_data and abs(correlation_data['file_size_mb']) > 0.5:
        print("File size strongly correlates with execution time")
        print("→ Consider file size-based load balancing")
    
    if 'memory_usage_mb' in correlation_data and abs(correlation_data['memory_usage_mb']) > 0.5:
        print("Memory usage affects performance")
        print("→ Consider memory optimization or streaming processing")


def main():
    print("Thread Performance Analyzer")
    print("=" * 50)
    
    if not os.path.exists(MEMORY_DIR):
        print(f"Directory not found: {MEMORY_DIR}")
        return
    
    file_data, thread_times = load_csv_files(MEMORY_DIR)
    
    if not thread_times:
        print("No thread performance data found")
        return
    
    df_combined, correlation_data = analyze_thread_performance(file_data, thread_times)
    create_visualizations(df_combined)
    identify_performance_issues(df_combined, correlation_data)
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)


if __name__ == "__main__":
    main()
