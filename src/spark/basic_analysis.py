# src/spark/basic_analysis.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
import pandas as pd


class BasicSalesAnalysis:
    def __init__(self):
        """初始化Spark会话"""
        self.spark = SparkSession.builder \
            .appName("EcommerceBasicAnalysis") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()

        # 设置日志级别
        self.spark.sparkContext.setLogLevel("WARN")

        print("Spark会话创建成功!")

    def load_data(self):
        """加载数据 - 修复Parquet格式问题，直接使用CSV格式"""
        print("\n=== 加载数据 ===")

        # 优先尝试CSV格式
        csv_path = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/processed/sales_data_cleaned.csv'
        excel_path = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/raw/商城详细销售数据.xlsx'

        if os.path.exists(csv_path):
            print(f"加载CSV文件: {csv_path}")
            self.df = self.spark.read.csv(
                csv_path,
                header=True,
                inferSchema=True,
                encoding='utf-8'
            )
        elif os.path.exists(excel_path):
            print(f"CSV文件不存在，加载Excel文件: {excel_path}")
            # 使用pandas读取Excel，然后转换为Spark DataFrame
            pd_df = pd.read_excel(excel_path)
            self.df = self.spark.createDataFrame(pd_df)

            # 保存为CSV供后续使用
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            pd_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"Excel数据已转换为CSV并保存: {csv_path}")
        else:
            print("错误：找不到任何数据文件！")
            print(f"请检查以下文件是否存在:")
            print(f"1. {csv_path}")
            print(f"2. {excel_path}")
            raise FileNotFoundError("找不到数据文件")

        print(f"数据加载完成: {self.df.count()} 条记录")

        # 显示数据结构
        print("\n数据结构:")
        self.df.printSchema()

        # 显示前5行
        print("\n数据示例:")
        self.df.limit(5).show()

        # 进行数据预处理
        self.preprocess_data()

        return self.df

    def preprocess_data(self):
        """数据预处理：添加分析所需的列"""
        print("\n=== 数据预处理 ===")

        # 检查关键列是否存在
        required_cols = ['销售额', '利润']
        existing_cols = [col for col in self.df.columns if col in required_cols]

        if len(existing_cols) < len(required_cols):
            print(f"警告：缺少某些列，现有列: {self.df.columns}")

        # 1. 解析日期列（如果存在）
        date_columns = []
        for date_col in ['订单日期', '发货日期']:
            if date_col in self.df.columns:
                try:
                    # 尝试多种日期格式
                    self.df = self.df.withColumn(
                        date_col,
                        to_date(col(date_col), 'yyyy/M/d')
                    )
                    date_columns.append(date_col)
                    print(f"已解析日期列: {date_col}")
                except Exception as e:
                    print(f"解析{date_col}失败: {str(e)}")
                    # 尝试其他格式
                    try:
                        self.df = self.df.withColumn(
                            date_col,
                            to_date(col(date_col), 'yyyy-MM-dd')
                        )
                        date_columns.append(date_col)
                        print(f"使用备用格式解析{date_col}成功")
                    except:
                        print(f"无法解析{date_col}，跳过")

        # 2. 添加时间维度（如果订单日期存在）
        if '订单日期' in self.df.columns and '订单日期' in date_columns:
            self.df = self.df.withColumn('年份', year(col('订单日期')))
            self.df = self.df.withColumn('月份', month(col('订单日期')))
            self.df = self.df.withColumn('季度', quarter(col('订单日期')))
            print("已添加时间维度列: 年份, 月份, 季度")

        # 3. 计算发货时长（如果日期列都存在）
        if all(col in date_columns for col in ['订单日期', '发货日期']):
            self.df = self.df.withColumn(
                '发货时长_天',
                datediff(col('发货日期'), col('订单日期'))
            )
            print("已计算发货时长")

        # 4. 计算利润率（如果销售额和利润都存在）
        if all(col in self.df.columns for col in ['销售额', '利润']):
            self.df = self.df.withColumn(
                '利润率',
                when(col('销售额') != 0, col('利润') / col('销售额')).otherwise(0)
            )
            print("已计算利润率")

        # 5. 确保数值列的格式
        numeric_cols = ['销售额', '利润', '数量', '折扣']
        for col_name in numeric_cols:
            if col_name in self.df.columns:
                try:
                    self.df = self.df.withColumn(col_name, col(col_name).cast('double'))
                except:
                    pass

        print("数据预处理完成")
        return self.df

    def calculate_kpis(self):
        """计算关键绩效指标"""
        print("\n=== 关键绩效指标 (KPIs) ===")

        # 检查必要列是否存在
        required_cols = ['销售额', '利润']
        available_cols = [col for col in required_cols if col in self.df.columns]

        if len(available_cols) < len(required_cols):
            print(f"警告：缺少必要的列，只能计算部分KPI")
            print(f"可用列: {self.df.columns}")

        # 构建聚合表达式
        agg_exprs = [count("*").alias("总订单数")]

        if '销售额' in self.df.columns:
            agg_exprs.append(sum("销售额").alias("总销售额"))

        if '利润' in self.df.columns:
            agg_exprs.append(sum("利润").alias("总利润"))

        if '客户 ID' in self.df.columns:
            agg_exprs.append(countDistinct("客户 ID").alias("总客户数"))

        if '产品 ID' in self.df.columns:
            agg_exprs.append(countDistinct("产品 ID").alias("总产品数"))

        if '利润率' in self.df.columns:
            agg_exprs.append(avg("利润率").alias("平均利润率"))

        kpis = self.df.agg(*agg_exprs)

        print("整体KPI:")
        kpis.show(truncate=False)

        # 转换为Pandas并保存
        try:
            kpis_pd = kpis.toPandas()
            os.makedirs('C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results', exist_ok=True)
            kpis_pd.to_csv('C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/kpis.csv',
                           index=False, encoding='utf-8-sig')
            print("KPI结果已保存至: C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/kpis.csv")
        except Exception as e:
            print(f"保存KPI结果失败: {str(e)}")

        return kpis

    def analyze_sales_trend(self):
        """分析销售趋势"""
        print("\n=== 销售趋势分析 ===")

        # 检查是否有年份列
        if '年份' not in self.df.columns:
            print("警告：缺少年份列，无法进行时间趋势分析")
            return None, None

        # 年度趋势
        yearly_trend = self.df.groupBy("年份") \
            .agg(
            count("*").alias("订单数"),
            sum("销售额").alias("年销售额"),
            sum("利润").alias("年利润"),
            avg("利润率").alias("平均利润率")
        ) \
            .orderBy("年份")

        print("年度销售趋势:")
        yearly_trend.show()

        # 月度趋势（最近一年）
        try:
            latest_year = self.df.agg(max("年份")).collect()[0][0]

            monthly_trend = self.df.filter(col("年份") == latest_year) \
                .groupBy("月份") \
                .agg(
                count("*").alias("订单数"),
                sum("销售额").alias("月销售额"),
                sum("利润").alias("月利润")
            ) \
                .orderBy("月份")

            print(f"\n{latest_year}年月度趋势:")
            monthly_trend.show()

            # 保存趋势结果
            yearly_trend.toPandas().to_csv(
                'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/yearly_trend.csv',
                index=False, encoding='utf-8-sig')
            monthly_trend.toPandas().to_csv(
                'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/monthly_trend.csv',
                index=False, encoding='utf-8-sig')

            print("趋势分析结果已保存")

            return yearly_trend, monthly_trend

        except Exception as e:
            print(f"月度趋势分析失败: {str(e)}")
            return yearly_trend, None

    def analyze_regions(self):
        """分析地区销售情况"""
        print("\n=== 地区销售分析 ===")

        # 检查是否有地区相关列
        region_cols = []
        for col_name in ['地区', '省/自治区', '省份', '省']:
            if col_name in self.df.columns:
                region_cols.append(col_name)

        if len(region_cols) < 1:
            print("警告：缺少地区相关列，跳过地区分析")
            return None

        group_cols = region_cols[:2] if len(region_cols) >= 2 else [region_cols[0]]

        region_analysis = self.df.groupBy(*group_cols) \
            .agg(
            count("*").alias("订单数"),
            sum("销售额").alias("地区销售额"),
            sum("利润").alias("地区利润"),
            avg("利润率").alias("平均利润率")
        ) \
            .orderBy(desc("地区销售额"))

        print(f"地区销售排行 (Top 10) - 按{group_cols}:")
        region_analysis.limit(10).show(truncate=False)

        # 保存结果
        try:
            region_analysis.toPandas().to_csv(
                'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/region_analysis.csv',
                index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"保存地区分析结果失败: {str(e)}")

        return region_analysis

    def analyze_categories(self):
        """分析产品品类"""
        print("\n=== 产品品类分析 ===")

        # 检查是否有品类相关列
        category_cols = []
        for col_name in ['类别', '品类', '产品类别']:
            if col_name in self.df.columns:
                category_cols.append(col_name)
                break

        subcategory_cols = []
        for col_name in ['子类别', '子品类', '产品子类别']:
            if col_name in self.df.columns:
                subcategory_cols.append(col_name)
                break

        if not category_cols:
            print("警告：缺少类别相关列，跳过品类分析")
            return None

        group_by_list = [category_cols[0]]
        if subcategory_cols:
            group_by_list.append(subcategory_cols[0])

        category_analysis = self.df.groupBy(*group_by_list) \
            .agg(
            count("*").alias("销量"),
            sum("销售额").alias("品类销售额"),
            sum("利润").alias("品类利润"),
            avg("利润率").alias("平均利润率")
        ) \
            .orderBy(desc("品类销售额"))

        print("产品品类分析 (Top 10):")
        category_analysis.limit(10).show(truncate=False)

        # 保存结果
        try:
            category_analysis.toPandas().to_csv(
                'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/category_analysis.csv',
                index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"保存品类分析结果失败: {str(e)}")

        return category_analysis

    def analyze_customers(self):
        """分析客户价值"""
        print("\n=== 客户价值分析 ===")

        # 检查是否有客户相关列
        customer_id_cols = []
        for col_name in ['客户 ID', '客户ID', '客户编号']:
            if col_name in self.df.columns:
                customer_id_cols.append(col_name)
                break

        if not customer_id_cols:
            print("警告：缺少客户ID列，跳过客户分析")
            return None

        customer_id_col = customer_id_cols[0]

        # 构建分组列
        group_cols = [customer_id_col]
        if '客户名称' in self.df.columns:
            group_cols.append('客户名称')
        if '细分' in self.df.columns:
            group_cols.append('细分')

        customer_metrics = self.df.groupBy(*group_cols) \
            .agg(
            count("*").alias("购买次数"),
            sum("销售额").alias("总消费金额"),
            sum("利润").alias("贡献利润"),
            avg("利润率").alias("平均利润率")
        ) \
            .orderBy(desc("总消费金额"))

        print("客户价值分析 (Top 10):")
        customer_metrics.limit(10).show(truncate=False)

        # 保存结果
        try:
            customer_metrics.toPandas().to_csv(
                'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/customer_analysis.csv',
                index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"保存客户分析结果失败: {str(e)}")

        return customer_metrics

    def analyze_shipping(self):
        """分析邮寄方式"""
        print("\n=== 邮寄方式分析 ===")

        # 检查是否有邮寄方式列
        shipping_cols = []
        for col_name in ['邮寄方式', '配送方式', '运输方式', '发货方式']:
            if col_name in self.df.columns:
                shipping_cols.append(col_name)
                break

        if not shipping_cols:
            print("警告：缺少邮寄方式列，跳过邮寄分析")
            return None

        shipping_col = shipping_cols[0]

        shipping_analysis = self.df.groupBy(shipping_col) \
            .agg(
            count("*").alias("订单数"),
            sum("销售额").alias("总销售额"),
            sum("利润").alias("总利润"),
            avg("利润率").alias("平均利润率")
        ) \
            .orderBy(desc("订单数"))

        print("邮寄方式对比:")
        shipping_analysis.show(truncate=False)

        # 保存结果
        try:
            shipping_analysis.toPandas().to_csv(
                'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/shipping_analysis.csv',
                index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"保存邮寄分析结果失败: {str(e)}")

        return shipping_analysis

    def run_analysis(self):
        """运行完整分析流程"""
        print("=== 开始电商销售基础分析 ===")

        try:
            # 1. 加载数据
            self.load_data()

            # 2. 计算KPI
            kpis = self.calculate_kpis()

            # 3. 销售趋势分析
            yearly_trend, monthly_trend = self.analyze_sales_trend()

            # 4. 地区分析
            region_analysis = self.analyze_regions()

            # 5. 品类分析
            category_analysis = self.analyze_categories()

            # 6. 客户分析
            customer_analysis = self.analyze_customers()

            # 7. 邮寄方式分析
            shipping_analysis = self.analyze_shipping()

            print("\n" + "=" * 50)
            print("=== 基础分析完成 ===")
            print("=" * 50)

            results = {
                'kpis': kpis,
                'yearly_trend': yearly_trend,
                'monthly_trend': monthly_trend,
                'region_analysis': region_analysis,
                'category_analysis': category_analysis,
                'customer_analysis': customer_analysis,
                'shipping_analysis': shipping_analysis
            }

            return results

        except Exception as e:
            print(f"分析过程中出现错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            # 停止Spark会话
            try:
                self.spark.stop()
                print("Spark会话已停止")
            except:
                pass


def main():
    """主函数"""
    analyzer = BasicSalesAnalysis()
    results = analyzer.run_analysis()

    if results:
        print("\n所有分析结果已保存至以下目录:")
        print("C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results/")

        # 列出生成的文件
        results_dir = 'C:/Users/xyh/ecommerce-analysis/ecommerce-analysis/data/results'
        if os.path.exists(results_dir):
            print("\n生成的文件:")
            for file in os.listdir(results_dir):
                if file.endswith('.csv'):
                    print(f"  - {file}")
        else:
            print(f"警告：结果目录不存在: {results_dir}")
    else:
        print("分析失败，请检查错误信息")


if __name__ == "__main__":
    main()