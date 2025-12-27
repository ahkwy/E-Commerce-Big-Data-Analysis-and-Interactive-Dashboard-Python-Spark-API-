# src/spark/rfm_analysis.py
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')


class RFMAnalysisPandas:
    def __init__(self):
        """初始化Pandas RFM分析器 - 完全避免Spark问题"""
        print("初始化Pandas RFM分析器...")

    def load_data_pandas(self):
        """使用Pandas加载数据"""
        print("\n=== 加载数据 ===")

        # 定义可能的文件路径
        csv_path = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/processed/sales_data_cleaned.csv'
        excel_path = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/raw/商城详细销售数据.xlsx'

        try:
            if os.path.exists(csv_path):
                print(f"加载CSV文件: {csv_path}")
                df = pd.read_csv(csv_path, encoding='utf-8')
            elif os.path.exists(excel_path):
                print(f"加载Excel文件: {excel_path}")
                df = pd.read_excel(excel_path)
            else:
                print("警告：找不到数据文件，创建测试数据...")
                df = self.create_test_data_pandas()

            print(f"数据加载完成: {len(df)} 条记录")
            print(f"数据列名: {list(df.columns)}")

            # 预览数据
            print("\n数据预览 (前5行):")
            print(df.head())

            return df

        except Exception as e:
            print(f"加载数据失败: {str(e)}")
            print("创建测试数据...")
            return self.create_test_data_pandas()

    def create_test_data_pandas(self):
        """创建Pandas测试数据"""
        print("创建测试数据...")

        # 创建1000个客户的测试数据
        np.random.seed(42)  # 确保结果可重现
        n_customers = 1000
        n_transactions = 5000

        customers = [f"CUST-{i:04d}" for i in range(1, n_customers + 1)]
        customer_names = [f"客户_{i}" for i in range(1, n_customers + 1)]

        data = {
            '客户 ID': [],
            '客户名称': [],
            '订单日期': [],
            '销售额': [],
            '细分': [],
            '地区': [],
            '类别': [],
            '子类别': []
        }

        for i in range(n_transactions):
            cust_idx = np.random.randint(0, n_customers)
            data['客户 ID'].append(customers[cust_idx])
            data['客户名称'].append(customer_names[cust_idx])

            # 随机日期（2023-2024年）
            year = 2024 if np.random.random() > 0.3 else 2023
            month = np.random.randint(1, 13)
            day = np.random.randint(1, 29)
            data['订单日期'].append(f"{year}-{month:02d}-{day:02d}")

            # 销售额（100-10000之间）
            data['销售额'].append(round(np.random.uniform(100, 10000), 2))

            # 其他字段
            data['细分'].append(np.random.choice(['消费者', '公司', '小型企业'], p=[0.6, 0.3, 0.1]))
            data['地区'].append(np.random.choice(['华东', '华北', '华南', '华中', '西南', '西北', '东北']))
            data['类别'].append(np.random.choice(['办公用品', '技术', '家具']))
            data['子类别'].append(np.random.choice(['用品', '设备', '椅子', '桌子', '纸张', '电话']))

        df = pd.DataFrame(data)

        # 保存测试数据
        test_csv_path = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/test_sales_data.csv'
        os.makedirs(os.path.dirname(test_csv_path), exist_ok=True)
        df.to_csv(test_csv_path, index=False, encoding='utf-8-sig')
        print(f"测试数据已保存: {test_csv_path}")

        return df

    def calculate_rfm_pandas(self, df):
        """使用Pandas计算RFM指标"""
        print("\n=== 计算RFM指标 ===")

        # 数据预处理
        df_clean = df.copy()

        # 解析日期
        if '订单日期' in df_clean.columns:
            df_clean['订单日期'] = pd.to_datetime(df_clean['订单日期'], errors='coerce')

        # 获取最新订单日期
        latest_date = df_clean['订单日期'].max()
        print(f"最新订单日期: {latest_date.date()}")

        # 按客户聚合
        rfm_data = df_clean.groupby('客户 ID').agg({
            '订单日期': 'max',  # 最近购买日期
            '客户 ID': 'count',  # 购买频率
            '销售额': 'sum'  # 购买金额
        }).rename(columns={
            '订单日期': '最近购买日期',
            '客户 ID': '购买频率',
            '销售额': '购买金额'
        }).reset_index()

        # 计算R值（最近购买天数）
        rfm_data['R_天数'] = (latest_date - rfm_data['最近购买日期']).dt.days

        print(f"RFM基础指标计算完成: {len(rfm_data)} 个客户")

        # 添加其他客户信息
        if '客户名称' in df_clean.columns:
            customer_info = df_clean[['客户 ID', '客户名称']].drop_duplicates()
            rfm_data = pd.merge(rfm_data, customer_info, on='客户 ID', how='left')

        if '细分' in df_clean.columns:
            segment_info = df_clean[['客户 ID', '细分']].drop_duplicates()
            rfm_data = pd.merge(rfm_data, segment_info, on='客户 ID', how='left')

        if '地区' in df_clean.columns:
            region_info = df_clean[['客户 ID', '地区']].drop_duplicates()
            rfm_data = pd.merge(rfm_data, region_info, on='客户 ID', how='left')

        return rfm_data

    def calculate_rfm_scores_pandas(self, rfm_data):
        """计算RFM分数"""
        print("\n=== 计算RFM分数 ===")

        # 复制数据避免修改原数据
        rfm_scored = rfm_data.copy()

        # 计算分位数
        try:
            # R值：天数越小越好（5分表示最近购买）
            rfm_scored['R_Score'] = pd.qcut(
                rfm_scored['R_天数'],
                q=5,
                labels=[5, 4, 3, 2, 1],
                duplicates='drop'
            ).astype(int)

            # F值：频率越高越好
            rfm_scored['F_Score'] = pd.qcut(
                rfm_scored['购买频率'],
                q=5,
                labels=[1, 2, 3, 4, 5],
                duplicates='drop'
            ).astype(int)

            # M值：金额越大越好
            rfm_scored['M_Score'] = pd.qcut(
                rfm_scored['购买金额'],
                q=5,
                labels=[1, 2, 3, 4, 5],
                duplicates='drop'
            ).astype(int)

        except Exception as e:
            print(f"分位数切分失败，使用简单分段: {str(e)}")

            # 使用简单分段方法
            def simple_score_r(days):
                if days <= 30:
                    return 5
                elif days <= 60:
                    return 4
                elif days <= 90:
                    return 3
                elif days <= 180:
                    return 2
                else:
                    return 1

            def simple_score_f(freq):
                if freq >= 10:
                    return 5
                elif freq >= 5:
                    return 4
                elif freq >= 3:
                    return 3
                elif freq >= 2:
                    return 2
                else:
                    return 1

            def simple_score_m(amount):
                if amount >= 10000:
                    return 5
                elif amount >= 5000:
                    return 4
                elif amount >= 2000:
                    return 3
                elif amount >= 1000:
                    return 2
                else:
                    return 1

            rfm_scored['R_Score'] = rfm_scored['R_天数'].apply(simple_score_r)
            rfm_scored['F_Score'] = rfm_scored['购买频率'].apply(simple_score_f)
            rfm_scored['M_Score'] = rfm_scored['购买金额'].apply(simple_score_m)

        # 计算RFM总分和平均分
        rfm_scored['RFM_总分'] = rfm_scored['R_Score'] + rfm_scored['F_Score'] + rfm_scored['M_Score']
        rfm_scored['RFM_平均分'] = rfm_scored['RFM_总分'] / 3

        print(f"RFM分数计算完成")
        print(f"R分数分布: {rfm_scored['R_Score'].value_counts().to_dict()}")
        print(f"F分数分布: {rfm_scored['F_Score'].value_counts().to_dict()}")
        print(f"M分数分布: {rfm_scored['M_Score'].value_counts().to_dict()}")

        return rfm_scored

    def segment_customers_pandas(self, rfm_scored):
        """客户分群"""
        print("\n=== 客户分群 ===")

        segmented = rfm_scored.copy()

        # 基于RFM分数进行分群
        def assign_segment(row):
            r, f, m = row['R_Score'], row['F_Score'], row['M_Score']

            if r >= 4 and f >= 4 and m >= 4:
                return '高价值客户'
            elif r >= 3 and f >= 3 and m >= 3:
                return '重要保持客户'
            elif r >= 3 and m >= 3:
                return '重要发展客户'
            elif r <= 2 and f <= 2:
                return '需挽留客户'
            else:
                return '一般客户'

        segmented['客户分群'] = segmented.apply(assign_segment, axis=1)

        # 基于RFM总分进行价值等级划分
        def assign_value_grade(score):
            if score >= 12:
                return 'VIP客户'
            elif score >= 9:
                return '重要客户'
            elif score >= 6:
                return '普通客户'
            else:
                return '低价值客户'

        segmented['价值等级'] = segmented['RFM_总分'].apply(assign_value_grade)

        # 分群统计
        print("\n客户分群结果:")
        segment_counts = segmented['客户分群'].value_counts()
        for segment, count in segment_counts.items():
            percentage = count / len(segmented) * 100
            print(f"  {segment}: {count}人 ({percentage:.1f}%)")

        print(f"\n总客户数: {len(segmented)}")

        return segmented

    def analyze_segments_pandas(self, segmented):
        """分析各分群特征"""
        print("\n=== 客户分群分析 ===")

        # 各分群客户数
        segment_counts = segmented.groupby('客户分群').agg({
            '客户 ID': 'count',
            'R_Score': 'mean',
            'F_Score': 'mean',
            'M_Score': 'mean',
            '购买金额': ['mean', 'sum'],
            '购买频率': 'mean',
            'R_天数': 'mean'
        }).round(2)

        # 重命名列
        segment_counts.columns = ['客户数', '平均R分', '平均F分', '平均M分',
                                  '平均消费金额', '总消费金额', '平均购买频率', '平均未购买天数']

        # 按客户数排序
        segment_counts = segment_counts.sort_values('客户数', ascending=False)

        print("\n各分群详细统计:")
        print(segment_counts)

        # 各分群消费贡献
        total_sales = segmented['购买金额'].sum()
        segment_counts['消费占比(%)'] = (segment_counts['总消费金额'] / total_sales * 100).round(2)

        # 创建贡献分析表
        segment_contribution = segment_counts[['消费占比(%)', '平均购买频率', '平均未购买天数']].copy()
        segment_contribution = segment_contribution.sort_values('消费占比(%)', ascending=False)

        print("\n各分群消费贡献:")
        print(segment_contribution)

        return segment_counts, segment_contribution

    def generate_insights(self, segmented, segment_counts):
        """生成业务洞察"""
        print("\n=== 业务洞察 ===")

        insights = []

        # 1. 高价值客户洞察
        if '高价值客户' in segment_counts.index:
            hv_stats = segment_counts.loc['高价值客户']
            insights.append(f"🎯 高价值客户: {int(hv_stats['客户数'])}人，"
                            f"仅占客户总数的{(hv_stats['客户数'] / len(segmented) * 100):.1f}%，"
                            f"但贡献了{hv_stats['消费占比(%)']:.1f}%的销售额")

        # 2. 需挽留客户洞察
        if '需挽留客户' in segment_counts.index:
            rn_stats = segment_counts.loc['需挽留客户']
            insights.append(f"⚠️ 需挽留客户: {int(rn_stats['客户数'])}人，"
                            f"平均{rn_stats['平均未购买天数']:.0f}天未购买，"
                            f"建议制定召回策略")

        # 3. 重要发展客户洞察
        if '重要发展客户' in segment_counts.index:
            rd_stats = segment_counts.loc['重要发展客户']
            insights.append(f"📈 重要发展客户: {int(rd_stats['客户数'])}人，"
                            f"消费潜力大，可重点培养")

        # 4. 整体客户价值分布
        value_dist = segmented['价值等级'].value_counts()
        insights.append("💰 客户价值等级分布:")
        for value, count in value_dist.items():
            percentage = count / len(segmented) * 100
            insights.append(f"  • {value}: {count}人 ({percentage:.1f}%)")

        # 5. RFM分数分布
        rfm_stats = segmented[['R_Score', 'F_Score', 'M_Score']].mean()
        insights.append(f"📊 平均RFM分数: R={rfm_stats['R_Score']:.1f}, "
                        f"F={rfm_stats['F_Score']:.1f}, M={rfm_stats['M_Score']:.1f}")

        # 6. 购买行为洞察
        avg_frequency = segmented['购买频率'].mean()
        avg_amount = segmented['购买金额'].mean()
        insights.append(f"🛒 平均购买行为: 频率{avg_frequency:.1f}次，金额{avg_amount:,.0f}元")

        return insights

    def save_results_pandas(self, segmented, segment_counts, insights):
        """保存分析结果"""
        print("\n=== 保存分析结果 ===")

        try:
            results_dir = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results'
            os.makedirs(results_dir, exist_ok=True)

            # 1. 保存RFM详细数据
            rfm_path = os.path.join(results_dir, 'rfm_analysis.csv')
            segmented.to_csv(rfm_path, index=False, encoding='utf-8-sig')
            print(f"✓ RFM详细数据已保存: {rfm_path}")

            # 2. 保存分群统计
            segment_counts_path = os.path.join(results_dir, 'segment_counts.csv')
            segment_counts.to_csv(segment_counts_path, encoding='utf-8-sig')
            print(f"✓ 分群统计已保存: {segment_counts_path}")

            # 3. 生成并保存分析报告
            self.save_analysis_report(segmented, segment_counts, insights, results_dir)

            # 4. 生成可视化摘要
            self.generate_visual_summary(segmented, segment_counts, results_dir)

            print(f"\n所有结果已保存到: {results_dir}")

        except Exception as e:
            print(f"保存结果失败: {str(e)}")

    def save_analysis_report(self, segmented, segment_counts, insights, results_dir):
        """保存分析报告"""
        report = {
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_customers': int(len(segmented)),
            'total_sales': float(segmented['购买金额'].sum()),
            'segment_distribution': {},
            'rfm_statistics': {
                'R_mean': float(segmented['R_Score'].mean()),
                'F_mean': float(segmented['F_Score'].mean()),
                'M_mean': float(segmented['M_Score'].mean()),
                'R_days_mean': float(segmented['R_天数'].mean()),
                'frequency_mean': float(segmented['购买频率'].mean()),
                'amount_mean': float(segmented['购买金额'].mean())
            },
            'business_insights': insights,
            'recommendations': [
                "针对高价值客户：提供VIP服务，增加客户粘性",
                "针对需挽留客户：制定召回计划，发送优惠券",
                "针对重要发展客户：个性化推荐，提升购买频率",
                "针对一般客户：培养消费习惯，提升客单价"
            ]
        }

        # 添加分群分布
        for segment, row in segment_counts.iterrows():
            report['segment_distribution'][segment] = {
                'customer_count': int(row['客户数']),
                'percentage': float(row['客户数'] / len(segmented) * 100),
                'avg_amount': float(row['平均消费金额']),
                'sales_contribution': float(row['消费占比(%)']) if '消费占比(%)' in row else 0
            }

        # 保存JSON报告
        report_path = os.path.join(results_dir, 'rfm_report.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"✓ 分析报告已保存: {report_path}")

        # 保存文本报告
        txt_report_path = os.path.join(results_dir, 'rfm_summary.txt')
        with open(txt_report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write("RFM客户价值分析报告\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"分析时间: {report['analysis_date']}\n")
            f.write(f"总客户数: {report['total_customers']:,}\n")
            f.write(f"总销售额: {report['total_sales']:,.0f}元\n\n")

            f.write("客户分群分布:\n")
            for segment, stats in report['segment_distribution'].items():
                f.write(f"  {segment}: {stats['customer_count']}人 ({stats['percentage']:.1f}%)\n")

            f.write("\n业务洞察:\n")
            for insight in insights[:8]:  # 只保存前8个洞察
                f.write(f"  • {insight}\n")

            f.write("\n建议策略:\n")
            for rec in report['recommendations']:
                f.write(f"  • {rec}\n")

        print(f"✓ 文本摘要已保存: {txt_report_path}")

    def generate_visual_summary(self, segmented, segment_counts, results_dir):
        """生成可视化摘要（可选）"""
        try:
            import matplotlib.pyplot as plt

            # 创建可视化
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))

            # 1. 客户分群分布饼图
            segment_sizes = segment_counts['客户数']
            segment_labels = segment_counts.index
            axes[0, 0].pie(segment_sizes, labels=segment_labels, autopct='%1.1f%%', startangle=90)
            axes[0, 0].set_title('客户分群分布')

            # 2. 消费贡献条形图
            if '消费占比(%)' in segment_counts.columns:
                segment_counts_sorted = segment_counts.sort_values('消费占比(%)', ascending=False)
                axes[0, 1].barh(segment_counts_sorted.index, segment_counts_sorted['消费占比(%)'])
                axes[0, 1].set_xlabel('消费占比(%)')
                axes[0, 1].set_title('各分群消费贡献')

            # 3. RFM分数分布箱线图
            rfm_scores = segmented[['R_Score', 'F_Score', 'M_Score']]
            axes[1, 0].boxplot([rfm_scores['R_Score'], rfm_scores['F_Score'], rfm_scores['M_Score']])
            axes[1, 0].set_xticklabels(['R_Score', 'F_Score', 'M_Score'])
            axes[1, 0].set_ylabel('分数')
            axes[1, 0].set_title('RFM分数分布')

            # 4. 客户价值等级分布
            if '价值等级' in segmented.columns:
                value_counts = segmented['价值等级'].value_counts()
                axes[1, 1].bar(value_counts.index, value_counts.values)
                axes[1, 1].set_xlabel('价值等级')
                axes[1, 1].set_ylabel('客户数')
                axes[1, 1].set_title('客户价值等级分布')
                plt.setp(axes[1, 1].xaxis.get_majorticklabels(), rotation=45)

            plt.tight_layout()

            # 保存图表
            chart_path = os.path.join(results_dir, 'rfm_analysis_chart.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()

            print(f"✓ 可视化图表已保存: {chart_path}")

        except Exception as e:
            print(f"生成可视化失败（需安装matplotlib）: {str(e)}")

    def run_analysis(self):
        """运行完整RFM分析流程"""
        print("=" * 60)
        print("开始Pandas RFM客户价值分析")
        print("=" * 60)

        try:
            # 1. 加载数据
            df = self.load_data_pandas()

            # 2. 计算RFM指标
            rfm_data = self.calculate_rfm_pandas(df)

            # 3. 计算RFM分数
            rfm_scored = self.calculate_rfm_scores_pandas(rfm_data)

            # 4. 客户分群
            segmented = self.segment_customers_pandas(rfm_scored)

            # 5. 分析各分群
            segment_counts, segment_contribution = self.analyze_segments_pandas(segmented)

            # 6. 生成业务洞察
            insights = self.generate_insights(segmented, segment_counts)

            # 7. 保存结果
            self.save_results_pandas(segmented, segment_counts, insights)

            print("\n" + "=" * 60)
            print("🎉 RFM分析成功完成!")
            print("=" * 60)

            # 打印关键结果
            print(f"\n📊 分析摘要:")
            print(f"  总客户数: {len(segmented):,}")
            print(f"  总销售额: {segmented['购买金额'].sum():,.0f}元")
            print(f"  客户分群: {len(segment_counts)}个分群")

            return {
                'rfm_data': segmented,
                'segment_counts': segment_counts,
                'insights': insights
            }

        except Exception as e:
            print(f"\n❌ RFM分析失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return None


def main():
    """主函数"""
    print("=" * 60)
    print("电商客户RFM价值分析系统 (Pandas版)")
    print("=" * 60)

    # 检查必要包
    try:
        import pandas as pd
        import numpy as np
        print(f"✓ Pandas版本: {pd.__version__}")
        print(f"✓ NumPy版本: {np.__version__}")
    except ImportError as e:
        print(f"❌ 缺少必要包: {e}")
        print("请运行: pip install pandas numpy matplotlib")
        return

    # 创建分析器
    analyzer = RFMAnalysisPandas()

    # 运行分析
    results = analyzer.run_analysis()

    if results:
        print("\n✅ 分析完成! 生成的文件:")

        # 列出生成的文件
        results_dir = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results'
        if os.path.exists(results_dir):
            files = [f for f in os.listdir(results_dir) if f.startswith('rfm') or f.startswith('segment')]
            for file in sorted(files):
                file_path = os.path.join(results_dir, file)
                size = os.path.getsize(file_path)
                print(f"  📄 {file} ({size:,} bytes)")

        print(f"\n📁 结果目录: {results_dir}")
    else:
        print("\n❌ 分析失败")


if __name__ == "__main__":
    main()