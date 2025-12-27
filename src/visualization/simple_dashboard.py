# src/visualization/simple_dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# 页面配置
st.set_page_config(
    page_title="电商销售分析仪表板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS样式
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .kpi-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: bold;
        color: #2c3e50;
    }
    .kpi-label {
        font-size: 1rem;
        color: #7f8c8d;
        margin-top: 5px;
    }
</style>
""", unsafe_allow_html=True)


def load_data():
    """加载数据"""
    data_files = {
        'sales': 'data/processed/sales_data_cleaned.csv',
        'kpis': 'data/results/kpis.csv',
        'yearly_trend': 'data/results/yearly_trend.csv',
        'region_analysis': 'data/results/region_analysis.csv',
        'category_analysis': 'data/results/category_analysis.csv',
        'rfm_analysis': 'data/results/rfm_analysis.csv'
    }

    data = {}

    for key, file_path in data_files.items():
        try:
            if os.path.exists(file_path):
                data[key] = pd.read_csv(file_path, encoding='utf-8-sig')
                st.sidebar.success(f"✓ {key} 加载成功")
            else:
                st.sidebar.warning(f"⚠ {key} 文件不存在: {file_path}")
                # 创建模拟数据
                data[key] = create_sample_data(key)
        except Exception as e:
            st.sidebar.error(f"✗ {key} 加载失败: {e}")
            data[key] = create_sample_data(key)

    return data


def create_sample_data(data_type):
    """创建样本数据（如果文件不存在）"""
    if data_type == 'sales':
        # 创建模拟销售数据
        dates = pd.date_range('2022-01-01', '2024-12-31', periods=1000)
        return pd.DataFrame({
            '订单日期': np.random.choice(dates, 1000),
            '销售额': np.random.exponential(1000, 1000),
            '利润': np.random.normal(200, 100, 1000),
            '地区': np.random.choice(['华东', '华北', '华南', '华中', '西南', '西北', '东北'], 1000),
            '类别': np.random.choice(['办公用品', '技术', '家具'], 1000)
        })

    elif data_type == 'kpis':
        return pd.DataFrame({
            '总订单数': [15000],
            '总销售额': [41623456.78],
            '总利润': [1234567.89],
            '总客户数': [2345],
            '总产品数': [678],
            '平均利润率': [0.1234]
        })

    elif data_type == 'yearly_trend':
        return pd.DataFrame({
            '年份': [2021, 2022, 2023, 2024],
            '年销售额': [8500000, 9500000, 11500000, 12123456],
            '年利润': [250000, 320000, 410000, 1234567],
            '订单数': [3200, 3600, 4200, 3985]
        })

    else:
        return pd.DataFrame({'样本': [1, 2, 3]})


def display_kpis(data):
    """显示KPI指标"""
    st.markdown('<h1 class="main-header">📈 电商销售大数据分析仪表板</h1>', unsafe_allow_html=True)

    # 从数据中获取KPI值
    if 'kpis' in data:
        kpis = data['kpis']
        total_sales = kpis['总销售额'].iloc[0] if '总销售额' in kpis.columns else 0
        total_profit = kpis['总利润'].iloc[0] if '总利润' in kpis.columns else 0
        total_orders = kpis['总订单数'].iloc[0] if '总订单数' in kpis.columns else 0
        total_customers = kpis['总客户数'].iloc[0] if '总客户数' in kpis.columns else 0
    else:
        total_sales = 41623456.78
        total_profit = 1234567.89
        total_orders = 15000
        total_customers = 2345

    # 创建4列布局
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">¥{total_sales:,.0f}</div>
            <div class="kpi-label">总销售额</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">¥{total_profit:,.0f}</div>
            <div class="kpi-label">总利润</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{total_orders:,}</div>
            <div class="kpi-label">总订单数</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-value">{total_customers:,}</div>
            <div class="kpi-label">客户总数</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")


def create_sales_trend_chart(data):
    """创建销售趋势图表"""
    st.subheader("📈 销售趋势分析")

    if 'yearly_trend' in data:
        df = data['yearly_trend']

        # 创建双Y轴图表
        fig = go.Figure()

        # 销售额柱状图
        fig.add_trace(go.Bar(
            x=df['年份'],
            y=df['年销售额'],
            name='销售额',
            marker_color='#1f77b4',
            yaxis='y'
        ))

        # 利润折线图
        fig.add_trace(go.Scatter(
            x=df['年份'],
            y=df['年利润'],
            name='利润',
            line=dict(color='#ff7f0e', width=3),
            yaxis='y2'
        ))

        # 更新布局
        fig.update_layout(
            height=400,
            xaxis_title='年份',
            yaxis_title='销售额 (元)',
            yaxis2=dict(
                title='利润 (元)',
                overlaying='y',
                side='right'
            ),
            legend=dict(x=0, y=1.1, orientation='h'),
            hovermode='x unified'
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("销售趋势数据暂不可用")


def create_region_analysis_chart(data):
    """创建地区分析图表"""
    st.subheader("🗺️ 地区销售分析")

    col1, col2 = st.columns(2)

    with col1:
        if 'region_analysis' in data:
            df = data['region_analysis']

            # 按地区汇总
            if '地区' in df.columns and '地区销售额' in df.columns:
                region_summary = df.groupby('地区')['地区销售额'].sum().reset_index()

                # 饼图
                fig = px.pie(
                    region_summary,
                    values='地区销售额',
                    names='地区',
                    title='各地区销售占比',
                    hole=0.3,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("地区数据格式不正确")
        else:
            st.info("地区分析数据暂不可用")

    with col2:
        if 'region_analysis' in data:
            df = data['region_analysis']

            # 取销售额Top 10省份
            if '省/自治区' in df.columns and '地区销售额' in df.columns:
                top_provinces = df.nlargest(10, '地区销售额')[['省/自治区', '地区销售额', '地区利润']]

                # 条形图
                fig = px.bar(
                    top_provinces,
                    x='地区销售额',
                    y='省/自治区',
                    orientation='h',
                    title='省份销售额Top 10',
                    color='地区利润',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("省份数据格式不正确")
        else:
            st.info("地区分析数据暂不可用")


def create_category_analysis_chart(data):
    """创建品类分析图表"""
    st.subheader("📦 产品品类分析")

    if 'category_analysis' in data:
        df = data['category_analysis']

        # 按品类汇总
        if '类别' in df.columns and '品类销售额' in df.columns:
            category_summary = df.groupby('类别').agg({
                '品类销售额': 'sum',
                '品类利润': 'sum',
                '销量': 'sum'
            }).reset_index()

            # 计算利润率
            category_summary['利润率'] = category_summary['品类利润'] / category_summary['品类销售额']

            # 创建散点图（气泡图）
            fig = px.scatter(
                category_summary,
                x='品类销售额',
                y='利润率',
                size='销量',
                color='类别',
                hover_name='类别',
                size_max=60,
                title='产品品类矩阵分析',
                labels={
                    '品类销售额': '销售额 (元)',
                    '利润率': '利润率'
                }
            )

            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("品类数据格式不正确")
    else:
        st.info("品类分析数据暂不可用")


def create_rfm_analysis_chart(data):
    """创建RFM分析图表"""
    st.subheader("👥 客户价值分析 (RFM)")

    if 'rfm_analysis' in data:
        df = data['rfm_analysis']

        # 检查必要的列
        required_cols = ['客户分群', 'R_Score', 'F_Score', 'M_Score']
        if all(col in df.columns for col in required_cols):
            # 分群统计
            segment_counts = df['客户分群'].value_counts().reset_index()
            segment_counts.columns = ['客户分群', '客户数']

            col1, col2 = st.columns(2)

            with col1:
                # 客户分群饼图
                fig1 = px.pie(
                    segment_counts,
                    values='客户数',
                    names='客户分群',
                    title='客户分群分布',
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig1.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                # RFM雷达图（平均分）
                segment_avg = df.groupby('客户分群')[['R_Score', 'F_Score', 'M_Score']].mean().reset_index()

                # 取前3个分群
                top_segments = segment_avg.head(3)

                fig2 = go.Figure()

                for _, row in top_segments.iterrows():
                    fig2.add_trace(go.Scatterpolar(
                        r=[row['R_Score'], row['F_Score'], row['M_Score'], row['R_Score']],
                        theta=['R(最近购买)', 'F(购买频率)', 'M(消费金额)', 'R(最近购买)'],
                        name=row['客户分群'],
                        fill='toself'
                    ))

                fig2.update_layout(
                    polar=dict(
                        radialaxis=dict(
                            visible=True,
                            range=[0, 5]
                        )),
                    showlegend=True,
                    title='客户分群RFM特征对比',
                    height=400
                )

                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("RFM数据格式不正确")
    else:
        st.info("RFM分析数据暂不可用")


def create_profit_analysis(data):
    """创建利润分析"""
    st.subheader("💰 利润深度分析")

    if 'sales' in data:
        df = data['sales']

        # 检查必要列
        if '利润' in df.columns and '利润率' in df.columns:
            # 创建两列布局
            col1, col2 = st.columns(2)

            with col1:
                # 利润分布直方图
                fig1 = px.histogram(
                    df,
                    x='利润',
                    nbins=50,
                    title='利润分布',
                    labels={'利润': '利润 (元)'},
                    color_discrete_sequence=['#2ca02c']
                )
                fig1.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="盈亏平衡线")
                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                # 利润率箱线图
                fig2 = px.box(
                    df,
                    y='利润率',
                    title='利润率分布',
                    points=False,
                    color_discrete_sequence=['#d62728']
                )
                fig2.add_hline(y=0, line_dash="dash", line_color="red", annotation_text="零利润线")
                st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("利润数据格式不正确")
    else:
        st.info("销售数据暂不可用")


def main():
    """主函数"""
    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 控制面板")

        st.markdown("---")
        st.subheader("数据筛选")

        # 年份筛选
        years = st.multiselect(
            "选择年份",
            options=[2021, 2022, 2023, 2024],
            default=[2022, 2023, 2024]
        )

        # 地区筛选
        regions = st.multiselect(
            "选择地区",
            options=["华东", "华北", "华南", "华中", "西南", "西北", "东北"],
            default=["华东", "华北", "华南"]
        )

        st.markdown("---")
        st.subheader("仪表板控制")

        # 自动刷新
        auto_refresh = st.checkbox("自动刷新数据", value=False)
        refresh_interval = st.slider("刷新间隔(秒)", 10, 300, 60, disabled=not auto_refresh)

        st.markdown("---")
        st.info(f"最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 数据重新加载按钮
        if st.button("🔄 重新加载数据"):
            st.cache_data.clear()
            st.rerun()

    # 加载数据
    data = load_data()

    # 显示KPI
    display_kpis(data)

    # 第一行图表
    create_sales_trend_chart(data)

    # 第二行图表
    create_region_analysis_chart(data)

    # 第三行图表
    create_category_analysis_chart(data)

    # 第四行图表
    create_rfm_analysis_chart(data)

    # 第五行图表
    create_profit_analysis(data)

    # 底部信息
    st.markdown("---")
    st.markdown("""
    ### 📋 数据说明
    - **数据来源**: 模拟生成的电商销售数据
    - **数据规模**: 9500+ 条销售记录
    - **分析工具**: Apache Spark + Python + Streamlit
    - **更新时间**: 实时分析，数据动态更新
    """)


if __name__ == "__main__":
    main()