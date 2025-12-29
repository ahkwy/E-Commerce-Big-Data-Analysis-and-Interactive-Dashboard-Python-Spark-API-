# src/visualization/simple_dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import warnings

warnings.filterwarnings('ignore')

# 在导入其他模块之前设置环境变量
os.environ['STREAMLIT_SERVER_HEADLESS'] = 'true'
os.environ['STREAMLIT_SERVER_ENABLE_STATIC_SERVING'] = 'true'

# 设置页面配置 - 必须在任何Streamlit命令之前调用
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
        transition: transform 0.3s ease;
    }
    .kpi-card:hover {
        transform: translateY(-5px);
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
    .filter-info {
        background-color: #e8f4f8;
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 15px;
        font-size: 0.9rem;
    }
    .stButton > button {
        background-color: #1f77b4;
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        font-weight: bold;
        transition: background-color 0.3s ease;
    }
    .stButton > button:hover {
        background-color: #1565c0;
    }
    /* 隐藏Streamlit的菜单和部署按钮 */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)  # 缓存5分钟
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
                # 使用st.success但不在加载时立即显示
                pass
            else:
                # 创建模拟数据
                data[key] = create_sample_data(key)
        except Exception as e:
            print(f"加载数据失败: {key} - {e}")
            data[key] = create_sample_data(key)

    return data


def create_sample_data(data_type):
    """创建样本数据（如果文件不存在）"""
    np.random.seed(42)

    if data_type == 'sales':
        # 创建更丰富的模拟销售数据
        n_records = 11000
        dates = pd.date_range('2021-01-01', '2024-12-31', periods=n_records)

        return pd.DataFrame({
            '订单日期': np.random.choice(dates, n_records),
            '年份': np.random.choice([2021, 2022, 2023, 2024], n_records),
            '月份': np.random.randint(1, 13, n_records),
            '销售额': np.random.exponential(1000, n_records),
            '利润': np.random.normal(200, 100, n_records),
            '利润率': np.random.uniform(-0.1, 0.3, n_records),
            '地区': np.random.choice(['华东', '华北', '华南', '华中', '西南', '西北', '东北'], n_records),
            '省/自治区': np.random.choice(
                ['北京', '上海', '广东', '浙江', '江苏', '四川', '湖北', '陕西', '山东', '河南'], n_records),
            '类别': np.random.choice(['办公用品', '技术', '家具'], n_records),
            '子类别': np.random.choice(['用品', '设备', '椅子', '桌子', '纸张', '电话'], n_records),
            '邮寄方式': np.random.choice(['标准级', '二级', '一级', '当日'], n_records, p=[0.6, 0.2, 0.15, 0.05]),
            '客户 ID': [f"CUST-{i:05d}" for i in range(n_records)],
            '细分': np.random.choice(['消费者', '公司', '小型企业'], n_records, p=[0.6, 0.3, 0.1])
        })

    elif data_type == 'kpis':
        return pd.DataFrame({
            '总订单数': [11000],
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
            '订单数': [2200, 2800, 3200, 2800]
        })

    elif data_type == 'region_analysis':
        regions = ['华东', '华北', '华南', '华中', '西南', '西北', '东北']
        return pd.DataFrame({
            '地区': regions,
            '地区销售额': [5123456, 4234567, 3345678, 2456789, 1567890, 1234567, 987654],
            '地区利润': [512345, 423456, 334567, 245678, 156789, 123456, 98765],
            '省/自治区': ['上海', '北京', '广东', '湖北', '四川', '陕西', '辽宁']
        })

    elif data_type == 'category_analysis':
        data = []
        for category in ['办公用品', '技术', '家具']:
            for _ in range(3):
                data.append({
                    '类别': category,
                    '子类别': np.random.choice(['用品', '设备', '椅子', '桌子', '纸张', '电话']),
                    '品类销售额': np.random.uniform(100000, 1000000),
                    '品类利润': np.random.uniform(10000, 200000),
                    '销量': np.random.randint(100, 5000)
                })
        return pd.DataFrame(data)

    elif data_type == 'rfm_analysis':
        segments = ['高价值客户', '重要保持客户', '重要发展客户', '一般客户', '需挽留客户']
        data = []
        for i in range(100):
            data.append({
                '客户分群': np.random.choice(segments),
                'R_Score': np.random.randint(1, 6),
                'F_Score': np.random.randint(1, 6),
                'M_Score': np.random.randint(1, 6),
                'R_天数': np.random.randint(1, 365),
                '购买频率': np.random.randint(1, 20),
                '购买金额': np.random.uniform(1000, 50000),
                '地区': np.random.choice(['华东', '华北', '华南', '华中', '西南', '西北', '东北'])
            })
        return pd.DataFrame(data)

    else:
        return pd.DataFrame({'样本': [1, 2, 3]})


def apply_filters(data, selected_years, selected_regions):
    """应用筛选条件到数据"""
    filtered_data = {}

    for key, df in data.items():
        if df is None or len(df) == 0:
            filtered_data[key] = df
            continue

        df_filtered = df.copy()

        # 对销售数据进行筛选
        if key == 'sales':
            if '年份' in df_filtered.columns and selected_years:
                df_filtered = df_filtered[df_filtered['年份'].isin(selected_years)]

            if '地区' in df_filtered.columns and selected_regions:
                df_filtered = df_filtered[df_filtered['地区'].isin(selected_regions)]

        # 对地区分析数据进行筛选
        elif key == 'region_analysis' and selected_regions:
            if '地区' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['地区'].isin(selected_regions)]

        # 对RFM分析数据进行筛选
        elif key == 'rfm_analysis' and selected_regions:
            if '地区' in df_filtered.columns:
                df_filtered = df_filtered[df_filtered['地区'].isin(selected_regions)]

        filtered_data[key] = df_filtered

    return filtered_data


def calculate_filtered_kpis(filtered_data):
    """根据筛选后的数据计算KPI"""
    if 'sales' not in filtered_data or filtered_data['sales'].empty:
        return None

    sales_df = filtered_data['sales']

    kpis = {
        '总订单数': len(sales_df),
        '总销售额': sales_df['销售额'].sum() if '销售额' in sales_df.columns else 0,
        '总利润': sales_df['利润'].sum() if '利润' in sales_df.columns else 0,
        '总客户数': sales_df['客户 ID'].nunique() if '客户 ID' in sales_df.columns else 0,
        '总产品数': sales_df['类别'].nunique() if '类别' in sales_df.columns else 0,
        '平均利润率': (sales_df['利润率'].mean() if '利润率' in sales_df.columns else
                       (sales_df['利润'].sum() / sales_df['销售额'].sum()
                        if '销售额' in sales_df.columns and '利润' in sales_df.columns and sales_df[
                           '销售额'].sum() != 0 else 0))
    }

    return kpis


def calculate_yearly_trend(filtered_data):
    """计算年度趋势"""
    if 'sales' not in filtered_data or filtered_data['sales'].empty:
        return None

    sales_df = filtered_data['sales']

    if '年份' not in sales_df.columns or '销售额' not in sales_df.columns:
        return None

    yearly_trend = sales_df.groupby('年份').agg({
        '销售额': 'sum',
        '利润': 'sum',
        '客户 ID': 'count'
    }).reset_index()

    yearly_trend.columns = ['年份', '年销售额', '年利润', '订单数']
    yearly_trend = yearly_trend.sort_values('年份')

    return yearly_trend


def calculate_region_analysis(filtered_data):
    """计算地区分析"""
    if 'sales' not in filtered_data or filtered_data['sales'].empty:
        return None

    sales_df = filtered_data['sales']

    if '地区' not in sales_df.columns or '销售额' not in sales_df.columns:
        return None

    # 按地区汇总
    region_analysis = sales_df.groupby('地区').agg({
        '销售额': 'sum',
        '利润': 'sum',
        '客户 ID': 'count'
    }).reset_index()

    region_analysis.columns = ['地区', '地区销售额', '地区利润', '订单数']
    region_analysis = region_analysis.sort_values('地区销售额', ascending=False)

    return region_analysis


def calculate_category_analysis(filtered_data):
    """计算品类分析"""
    if 'sales' not in filtered_data or filtered_data['sales'].empty:
        return None

    sales_df = filtered_data['sales']

    if '类别' not in sales_df.columns or '销售额' not in sales_df.columns:
        return None

    # 按品类汇总
    category_analysis = sales_df.groupby('类别').agg({
        '销售额': 'sum',
        '利润': 'sum',
        '客户 ID': 'count'
    }).reset_index()

    category_analysis.columns = ['类别', '品类销售额', '品类利润', '销量']
    category_analysis['利润率'] = category_analysis['品类利润'] / category_analysis['品类销售额']
    category_analysis = category_analysis.sort_values('品类销售额', ascending=False)

    return category_analysis


def display_kpis(kpis_data, is_filtered=False):
    """显示KPI指标"""
    st.markdown('<h1 class="main-header">📈 电商销售大数据分析仪表板</h1>', unsafe_allow_html=True)

    # 显示筛选状态
    if is_filtered:
        st.markdown('<div class="filter-info">📊 当前显示筛选后的数据</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="filter-info">📊 当前显示全部数据</div>', unsafe_allow_html=True)

    if kpis_data is None:
        # 使用默认值
        total_sales = 41623456.78
        total_profit = 1234567.89
        total_orders = 11000
        total_customers = 2345
    else:
        total_sales = kpis_data.get('总销售额', 0)
        total_profit = kpis_data.get('总利润', 0)
        total_orders = kpis_data.get('总订单数', 0)
        total_customers = kpis_data.get('总客户数', 0)

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


def create_sales_trend_chart(trend_data):
    """创建销售趋势图表"""
    st.subheader("📈 销售趋势分析")

    if trend_data is not None and not trend_data.empty:
        df = trend_data

        # 创建双Y轴图表
        fig = go.Figure()

        # 销售额柱状图
        fig.add_trace(go.Bar(
            x=df['年份'],
            y=df['年销售额'],
            name='销售额',
            marker_color='#1f77b4',
            yaxis='y',
            text=df['年销售额'].apply(lambda x: f'¥{x:,.0f}'),
            textposition='auto'
        ))

        # 利润折线图
        fig.add_trace(go.Scatter(
            x=df['年份'],
            y=df['年利润'],
            name='利润',
            line=dict(color='#ff7f0e', width=3),
            yaxis='y2',
            text=df['年利润'].apply(lambda x: f'¥{x:,.0f}'),
            mode='lines+markers'
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
            hovermode='x unified',
            showlegend=True
        )

        st.plotly_chart(fig, use_container_width=True)

        # 显示数据表格
        with st.expander("查看详细数据"):
            st.dataframe(df)
    else:
        st.info("销售趋势数据暂不可用")


def create_region_analysis_chart(region_data):
    """创建地区分析图表"""
    st.subheader("🗺️ 地区销售分析")

    if region_data is not None and not region_data.empty:
        df = region_data

        col1, col2 = st.columns(2)

        with col1:
            if '地区' in df.columns and '地区销售额' in df.columns:
                # 饼图
                fig = px.pie(
                    df,
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

        with col2:
            if '地区' in df.columns and '地区销售额' in df.columns:
                # 条形图
                fig = px.bar(
                    df,
                    x='地区销售额',
                    y='地区',
                    orientation='h',
                    title='地区销售额排行',
                    color='地区利润',
                    color_continuous_scale='Blues',
                    text='地区销售额'
                )
                fig.update_traces(texttemplate='¥%{text:,.0f}', textposition='outside')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("地区数据格式不正确")
    else:
        st.info("地区分析数据暂不可用")


def create_category_analysis_chart(category_data):
    """创建品类分析图表"""
    st.subheader("📦 产品品类分析")

    if category_data is not None and not category_data.empty:
        df = category_data

        # 创建散点图（气泡图）
        fig = px.scatter(
            df,
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
            },
            hover_data={
                '品类销售额': ':,',
                '品类利润': ':,',
                '销量': ':',
                '利润率': ':.2%'
            }
        )

        fig.update_traces(
            hovertemplate='<br>'.join([
                '品类: %{hovertext}',
                '销售额: ¥%{x:,.0f}',
                '利润率: %{y:.2%}',
                '销量: %{marker.size:,}'
            ])
        )

        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

        # 显示数据表格
        with st.expander("查看品类详细数据"):
            st.dataframe(df)
    else:
        st.info("品类分析数据暂不可用")


def create_rfm_analysis_chart(rfm_data, is_filtered=False):
    """创建RFM分析图表"""
    st.subheader("👥 客户价值分析 (RFM)")

    if rfm_data is not None and not rfm_data.empty:
        df = rfm_data

        # 显示数据状态
        if is_filtered:
            st.info("📌 RFM分析基于全部客户数据，筛选仅影响地区显示")

        # 检查必要的列
        if '客户分群' in df.columns:
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
                # 如果有RFM分数，显示雷达图
                if all(col in df.columns for col in ['R_Score', 'F_Score', 'M_Score']):
                    # RFM平均分
                    segment_avg = df.groupby('客户分群')[['R_Score', 'F_Score', 'M_Score']].mean().reset_index()

                    # 取前3个分群
                    top_segments = segment_avg.head(3)

                    fig2 = go.Figure()

                    colors = px.colors.qualitative.Set3[:3]
                    for idx, (_, row) in enumerate(top_segments.iterrows()):
                        fig2.add_trace(go.Scatterpolar(
                            r=[row['R_Score'], row['F_Score'], row['M_Score'], row['R_Score']],
                            theta=['R(最近购买)', 'F(购买频率)', 'M(消费金额)', 'R(最近购买)'],
                            name=row['客户分群'],
                            fill='toself',
                            line_color=colors[idx]
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
                    # 显示客户分群统计表格
                    st.dataframe(segment_counts)

            # 显示洞察
            total_customers = len(df)
            if '高价值客户' in df['客户分群'].values:
                hv_count = len(df[df['客户分群'] == '高价值客户'])
                hv_percentage = (hv_count / total_customers) * 100
                st.success(f"💎 高价值客户: {hv_count}人 ({hv_percentage:.1f}%)")

            if '需挽留客户' in df['客户分群'].values:
                rn_count = len(df[df['客户分群'] == '需挽留客户'])
                rn_percentage = (rn_count / total_customers) * 100
                st.warning(f"⚠️ 需挽留客户: {rn_count}人 ({rn_percentage:.1f}%)，建议制定召回策略")
        else:
            st.info("RFM数据格式不正确")
            with st.expander("查看原始数据"):
                st.dataframe(df.head())
    else:
        st.info("RFM分析数据暂不可用")


def create_profit_analysis(sales_data):
    """创建利润分析"""
    st.subheader("💰 利润深度分析")

    if sales_data is not None and not sales_data.empty:
        df = sales_data

        # 检查必要列
        if '利润' in df.columns:
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
                # 如果有利润率数据，显示箱线图
                if '利润率' in df.columns:
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
                    # 计算利润率
                    if '销售额' in df.columns and '利润' in df.columns:
                        df['利润率'] = df['利润'] / df['销售额']
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
                        st.info("无法计算利润率数据")
        else:
            st.info("利润数据格式不正确")
    else:
        st.info("销售数据暂不可用")


def main():
    """主函数"""
    # 初始化session_state
    if 'selected_years' not in st.session_state:
        st.session_state.selected_years = [2022, 2023, 2024]
    if 'selected_regions' not in st.session_state:
        st.session_state.selected_regions = ["华东", "华北", "华南"]
    if 'last_filter' not in st.session_state:
        st.session_state.last_filter = ""

    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 控制面板")

        st.markdown("---")
        st.subheader("数据筛选")

        # 年份筛选
        selected_years = st.multiselect(
            "选择年份",
            options=[2021, 2022, 2023, 2024],
            default=st.session_state.selected_years,
            key="year_filter"
        )

        # 地区筛选
        selected_regions = st.multiselect(
            "选择地区",
            options=["华东", "华北", "华南", "华中", "西南", "西北", "东北"],
            default=st.session_state.selected_regions,
            key="region_filter"
        )

        # 更新session_state
        st.session_state.selected_years = selected_years
        st.session_state.selected_regions = selected_regions

        # 检查筛选是否变化
        current_filter = f"{selected_years}_{selected_regions}"
        filter_changed = current_filter != st.session_state.last_filter
        st.session_state.last_filter = current_filter

        st.markdown("---")
        st.subheader("仪表板控制")

        # 显示筛选信息
        if selected_years:
            st.info(f"📅 年份筛选: {', '.join(map(str, selected_years))}")
        else:
            st.warning("📅 未选择年份，显示全部年份")

        if selected_regions:
            st.info(f"🗺️ 地区筛选: {', '.join(selected_regions)}")
        else:
            st.warning("🗺️ 未选择地区，显示全部地区")

        st.markdown("---")
        st.info(f"最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        col1, col2 = st.columns(2)
        with col1:
            # 数据重新加载按钮
            if st.button("🔄 重新加载数据"):
                st.cache_data.clear()
                st.rerun()

        with col2:
            # 重置筛选按钮
            if st.button("🔄 重置筛选"):
                st.session_state.selected_years = [2022, 2023, 2024]
                st.session_state.selected_regions = ["华东", "华北", "华南"]
                st.rerun()

    # 加载数据
    data = load_data()

    # 在侧边栏显示数据加载状态
    with st.sidebar:
        st.markdown("---")
        st.subheader("数据状态")
        if all(len(df) > 0 for df in data.values()):
            st.success("✅ 所有数据已加载")
        else:
            st.warning("⚠️ 部分数据未加载，使用模拟数据")

    # 应用筛选条件
    is_filtered = bool(selected_years or selected_regions)
    filtered_data = apply_filters(data, selected_years, selected_regions)

    # 计算筛选后的数据
    kpis_data = calculate_filtered_kpis(filtered_data)
    yearly_trend_data = calculate_yearly_trend(filtered_data)
    region_analysis_data = calculate_region_analysis(filtered_data)
    category_analysis_data = calculate_category_analysis(filtered_data)

    # 获取RFM数据（不重新计算）
    rfm_data = filtered_data.get('rfm_analysis', None)

    # 显示KPI
    display_kpis(kpis_data, is_filtered)

    # 显示图表
    create_sales_trend_chart(yearly_trend_data)
    create_region_analysis_chart(region_analysis_data)
    create_category_analysis_chart(category_analysis_data)
    create_rfm_analysis_chart(rfm_data, is_filtered)
    create_profit_analysis(filtered_data.get('sales', None))

    # 底部信息
    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 📋 数据说明")
        st.markdown(f"""
        - **数据来源**: {'筛选后的模拟数据' if is_filtered else '全部模拟数据'}
        - **数据规模**: {kpis_data['总订单数'] if kpis_data else 11000}+ 条销售记录
        - **分析工具**: Apache Spark + Python + Streamlit
        - **更新时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """)

    with col2:
        st.markdown("### 📊 分析模块")
        st.markdown("""
        - 销售趋势分析
        - 地区分布分析
        - 产品品类分析
        - 客户价值分析
        - 利润深度分析
        """)

    with col3:
        st.markdown("### 🛠️ 技术栈")
        st.markdown("""
        - Python + Pandas
        - Streamlit 仪表板
        - Plotly 可视化
        - Spark 大数据处理
        """)

    st.markdown("---")
    st.caption("© 2024 电商销售分析系统 | 基于Spark大数据分析")


if __name__ == "__main__":
    # 运行主函数
    main()