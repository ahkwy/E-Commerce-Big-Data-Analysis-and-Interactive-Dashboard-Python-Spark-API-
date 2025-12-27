# src/utils/data_preparation.py
import pandas as pd
import numpy as np
from datetime import datetime
import os


class DataPreprocessor:
    def __init__(self, data_path='C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/processed/sales_data_processed.csv'):
        self.data_path = data_path
        self.df = None

    def load_data(self):
        """加载数据"""
        print(f"加载数据: {self.data_path}")
        self.df = pd.read_csv(self.data_path, encoding='utf-8-sig')
        print(f"数据加载完成: {len(self.df)} 条记录")
        return self.df

    def clean_data(self):
        """数据清洗"""
        print("开始数据清洗...")

        # 1. 处理日期
        self.df['订单日期'] = pd.to_datetime(self.df['订单日期'], errors='coerce')
        self.df['发货日期'] = pd.to_datetime(self.df['发货日期'], errors='coerce')

        # 2. 计算发货时长
        self.df['发货时长_天'] = (self.df['发货日期'] - self.df['订单日期']).dt.days

        # 3. 处理异常值
        # 移除负销售额
        self.df = self.df[self.df['销售额'] > 0]

        # 限制极端值
        self.df = self.df[self.df['销售额'] < 100000]  # 销售额 < 10万
        self.df = self.df[(self.df['利润'] > -10000) & (self.df['利润'] < 50000)]  # 利润在合理范围

        # 4. 处理缺失值
        self.df['折扣'] = self.df['折扣'].fillna(0)
        self.df['利润'] = self.df['利润'].fillna(0)

        print(f"数据清洗后: {len(self.df)} 条记录")
        return self.df

    def add_features(self):
        """添加衍生特征"""
        print("添加衍生特征...")

        # 1. 时间特征
        self.df['年份'] = self.df['订单日期'].dt.year
        self.df['月份'] = self.df['订单日期'].dt.month
        self.df['季度'] = self.df['订单日期'].dt.quarter
        self.df['星期'] = self.df['订单日期'].dt.dayofweek  # 0=周一, 6=周日

        # 2. 计算利润率
        self.df['利润率'] = np.where(
            self.df['销售额'] > 0,
            self.df['利润'] / self.df['销售额'],
            0
        )

        # 3. 标记亏损订单
        self.df['是否亏损'] = (self.df['利润'] < 0).astype(int)

        # 4. 计算订单价值
        self.df['平均单价'] = self.df['销售额'] / self.df['数量']

        print("衍生特征添加完成")
        return self.df

    def save_cleaned_data(self, output_csv='C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/processed/sales_data_cleaned.csv',
                          output_parquet='C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/processed/sales_data_cleaned.parquet'):
        """保存清洗后的数据"""
        print("保存清洗后的数据...")

        # 确保目录存在
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

        # 保存为CSV
        self.df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"CSV文件已保存: {output_csv}")

        # 保存为Parquet（Spark处理更高效）
        self.df.to_parquet(output_parquet, index=False)
        print(f"Parquet文件已保存: {output_parquet}")

        return output_csv, output_parquet

    def generate_report(self):
        """生成数据质量报告"""
        print("生成数据质量报告...")

        report = {
            '总记录数': len(self.df),
            '时间范围': {
                '开始': self.df['订单日期'].min().strftime('%Y-%m-%d'),
                '结束': self.df['订单日期'].max().strftime('%Y-%m-%d')
            },
            '销售额统计': {
                '总和': f"{self.df['销售额'].sum():,.2f}",
                '均值': f"{self.df['销售额'].mean():,.2f}",
                '中位数': f"{self.df['销售额'].median():,.2f}",
                '最大值': f"{self.df['销售额'].max():,.2f}",
                '最小值': f"{self.df['销售额'].min():,.2f}"
            },
            '利润统计': {
                '总和': f"{self.df['利润'].sum():,.2f}",
                '均值': f"{self.df['利润'].mean():,.2f}",
                '亏损订单数': int(self.df['是否亏损'].sum()),
                '亏损比例': f"{(self.df['是否亏损'].sum() / len(self.df) * 100):.2f}%"
            },
            '客户统计': {
                '总客户数': self.df['客户 ID'].nunique(),
                '总产品数': self.df['产品 ID'].nunique()
            },
            '地区分布': self.df['地区'].value_counts().to_dict(),
            '品类分布': self.df['类别'].value_counts().to_dict()
        }

        # 保存报告
        import json
        report_path = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/data_quality_report.json'

        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"数据质量报告已保存: {report_path}")

        # 打印报告摘要
        print("\n=== 数据质量报告摘要 ===")
        for section, content in report.items():
            if isinstance(content, dict):
                print(f"\n{section}:")
                for key, value in content.items():
                    print(f"  {key}: {value}")
            else:
                print(f"{section}: {content}")

    def run_pipeline(self):
        """运行完整的数据预处理流程"""
        print("=== 数据预处理流程开始 ===")

        # 1. 加载数据
        self.load_data()

        # 2. 数据清洗
        self.clean_data()

        # 3. 添加特征
        self.add_features()

        # 4. 保存清洗后的数据
        csv_path, parquet_path = self.save_cleaned_data()

        # 5. 生成报告
        self.generate_report()

        print("\n=== 数据预处理流程完成 ===")
        return self.df


def main():
    """主函数"""
    preprocessor = DataPreprocessor()
    df = preprocessor.run_pipeline()

    # 打印数据示例
    print("\n数据示例（前5行）:")
    print(df.head().to_string())

    print("\n数据列信息:")
    print(df.info())


if __name__ == "__main__":
    main()