# src/crawler/data_simulator.py
import pandas as pd
import numpy as np
import os
import json


class DataLoader:
    def __init__(self, data_path):
        """
        初始化数据加载器

        参数:
        - data_path: 数据文件路径
        """
        self.data_path = data_path

    def load_data(self):
        """加载数据"""
        if not os.path.exists(self.data_path):
            print(f"错误：数据文件不存在: {self.data_path}")
            return None

        print(f"正在加载数据文件: {self.data_path}")

        try:
            # 根据文件扩展名选择加载方式
            if self.data_path.endswith('.csv'):
                df = pd.read_csv(self.data_path, encoding='utf-8')
            elif self.data_path.endswith('.xlsx') or self.data_path.endswith('.xls'):
                df = pd.read_excel(self.data_path)
            elif self.data_path.endswith('.json'):
                df = pd.read_json(self.data_path)
            else:
                print(f"错误：不支持的文件格式: {self.data_path}")
                return None

            print(f"成功加载数据: {len(df)} 条记录")
            print(f"数据列名: {', '.join(df.columns.tolist())}")

            return df

        except Exception as e:
            print(f"加载数据时出错: {str(e)}")
            return None

    def generate_data_summary(self, df, output_path='C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/data_summary.json'):
        """生成数据摘要"""
        if df is None:
            print("无法生成摘要：数据为空")
            return

        print("\n正在生成数据摘要...")

        summary = {
            '总记录数': len(df),
            '数据列数': len(df.columns),
            '列名': df.columns.tolist(),
            '数据文件': self.data_path,
            '加载时间': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 添加数值列的基本统计
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            summary['数值列统计'] = {}
            for col in numeric_cols[:10]:  # 只显示前10个数值列
                summary['数值列统计'][col] = {
                    '最小值': float(df[col].min()),
                    '最大值': float(df[col].max()),
                    '平均值': float(df[col].mean()),
                    '中位数': float(df[col].median()),
                    '标准差': float(df[col].std())
                }

        # 添加分类列的统计
        object_cols = df.select_dtypes(include=['object']).columns.tolist()
        if object_cols:
            summary['分类列统计'] = {}
            for col in object_cols[:5]:  # 只显示前5个分类列
                unique_values = df[col].unique()[:10]  # 只显示前10个唯一值
                summary['分类列统计'][col] = {
                    '唯一值数量': int(df[col].nunique()),
                    '前10个唯一值': unique_values.tolist() if len(unique_values) > 0 else []
                }

        # 确保目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 保存摘要
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print(f"数据摘要已保存至: {output_path}")

        # 打印基本摘要
        print("\n=== 数据摘要 ===")
        print(f"总记录数: {summary['总记录数']}")
        print(f"数据列数: {summary['数据列数']}")
        print(f"数据文件: {summary['数据文件']}")

        if '数值列统计' in summary:
            print(f"\n数值列 ({len(summary['数值列统计'])}个): {', '.join(summary['数值列统计'].keys())}")

        if '分类列统计' in summary:
            print(f"分类列 ({len(summary['分类列统计'])}个): {', '.join(summary['分类列统计'].keys())}")

        return summary

    def save_processed_data(self, df, output_path='C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/processed/sales_data_processed.csv'):
        """保存处理后的数据"""
        if df is None:
            print("无法保存数据：数据为空")
            return

        # 确保目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # 保存数据
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"处理后的数据已保存至: {output_path}")

        return output_path


def main():
    """主函数"""
    print("=== 电商销售数据加载器 ===")

    # 数据文件路径
    data_path = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/raw/商城详细销售数据.xlsx'
    print(f"【调试】待加载的原始文件路径: {data_path}")
    print(f"【调试】文件是否存在: {os.path.exists(data_path)}")  # 关键：检查文件是否真的存在

    # 创建数据加载器实例
    loader = DataLoader(data_path)

    # 加载数据
    df = loader.load_data()
    print(f"【调试】加载后的数据框是否为空: {df is None}")  # 关键：检查数据是否加载成功

    if df is not None:
        # 生成数据摘要
        loader.generate_data_summary(df)

        # 保存处理后的数据（可选）
        processed_path = loader.save_processed_data(df)

        print(f"\n数据处理完成！")
        print(f"原始数据: {data_path}")
        print(f"处理后数据: {processed_path}")

        # 返回数据框供后续使用
        return df
    else:
        print("【错误】数据加载失败，无法生成任何文件！")


if __name__ == "__main__":
    main()