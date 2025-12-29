 #!/bin/bash
# run_analysis.sh

echo "=========================================="
echo "电商销售大数据分析项目 - 一键运行脚本"
echo "=========================================="

# 检查Python环境
echo "1. 检查Python环境..."
python --version
if [ $? -ne 0 ]; then
    echo "错误：Python未安装"
    exit 1
fi

# 创建目录结构
echo "2. 创建目录结构..."
mkdir -p data/raw data/processed data/results
mkdir -p outputs/visualizations logs

# 激活虚拟环境（如果存在）
if [ -d "venv" ]; then
    echo "3. 激活虚拟环境..."
    source venv/bin/activate
fi

# 安装依赖
echo "4. 安装Python依赖..."
pip install -r requirements.txt > logs/install.log 2>&1

# 生成示例数据（如果不存在）
echo "5. 准备数据..."
if [ ! -f "data/raw/商城详细销售数据.xlsx" ]; then
    echo "  生成示例数据..."
    python src/crawler/generate_sample_excel.py > logs/data_generation.log 2>&1
fi

# 数据模拟
echo "6. 生成扩展数据..."
python src/crawler/data_simulator.py > logs/data_simulation.log 2>&1

# 数据预处理
echo "7. 数据预处理..."
python src/utils/data_preparation.py > logs/data_preparation.log 2>&1

# Spark分析
echo "8. 运行Spark分析..."
echo "  注意：这可能需要几分钟时间"
python src/spark/basic_analysis.py > logs/basic_analysis.log 2>&1

# RFM分析
echo "9. 运行RFM分析..."
python src/spark/rfm_analysis.py > logs/rfm_analysis.log 2>&1

echo "=========================================="
echo "分析流程完成！"
echo ""
echo "生成的文件："
echo "  - data/processed/sales_data_cleaned.csv     # 清洗后的数据"
echo "  - data/results/                            # 分析结果"
echo "  - logs/                                    # 运行日志"
echo ""
echo "运行以下命令启动可视化仪表板："
echo "  streamlit run src/visualization/simple_dashboard.py"
echo ""
echo "然后在浏览器中访问：http://localhost:8501"
echo "=========================================="
