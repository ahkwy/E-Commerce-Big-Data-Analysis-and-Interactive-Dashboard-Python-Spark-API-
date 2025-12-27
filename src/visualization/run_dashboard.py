# launch_dashboard.py
import os
import sys
import subprocess


def main():
    """主函数"""
    print("=" * 60)
    print("电商销售分析仪表板启动器")
    print("=" * 60)

    # 项目根目录
    project_root = r"C:\Users\xyh\ecommerce-analysis\ecommerce-analysis"

    # 检查项目目录是否存在
    if not os.path.exists(project_root):
        print(f"✗ 项目目录不存在: {project_root}")
        print("请检查路径是否正确")
        return

    # 切换到项目根目录
    print(f"切换到项目目录: {project_root}")
    os.chdir(project_root)

    # 仪表板文件路径（相对路径）
    dashboard_relative = "src/visualization/simple_dashboard.py"
    dashboard_absolute = os.path.join(project_root, dashboard_relative)

    # 检查文件是否存在
    if not os.path.exists(dashboard_absolute):
        print(f"✗ 仪表板文件不存在: {dashboard_absolute}")
        print("\n当前目录内容:")
        for item in os.listdir("."):
            print(f"  {item}")

        if os.path.exists("src"):
            print("\nsrc目录内容:")
            for item in os.listdir("src"):
                print(f"  {item}")

                viz_path = os.path.join("src", item, "simple_dashboard.py")
                if os.path.exists(viz_path):
                    print(f"✓ 找到仪表板文件: {viz_path}")
                    dashboard_relative = viz_path

        if not os.path.exists(dashboard_absolute):
            print("\n请检查文件结构:")
            os.system("tree /f" if os.name == 'nt' else "find . -name '*.py'")
            return

    print(f"✓ 找到仪表板文件: {dashboard_relative}")

    # 检查依赖
    print("\n检查依赖...")
    try:
        import streamlit
        import pandas
        import plotly
        print("✓ 所有依赖已安装")
    except ImportError as e:
        print(f"✗ 缺少依赖: {e}")
        print("请运行: pip install streamlit pandas plotly numpy")
        install = input("是否自动安装？(y/n): ").lower()
        if install == 'y':
            subprocess.run([sys.executable, "-m", "pip", "install", "streamlit", "pandas", "plotly", "numpy"])
        else:
            return

    # 运行Streamlit
    print("\n" + "=" * 60)
    print("启动仪表板...")
    print("访问地址: http://localhost:8501")
    print("按 Ctrl+C 停止仪表板")
    print("=" * 60 + "\n")

    try:
        # 使用相对路径运行Streamlit
        subprocess.run(["streamlit", "run", dashboard_relative])
    except KeyboardInterrupt:
        print("\n仪表板已停止")
    except FileNotFoundError:
        print("\n✗ 找不到streamlit命令")
        print("请确保已安装streamlit: pip install streamlit")
    except Exception as e:
        print(f"\n启动失败: {e}")
        print("\n尝试直接运行命令:")
        print(f'streamlit run "{dashboard_relative}"')


if __name__ == "__main__":
    main()