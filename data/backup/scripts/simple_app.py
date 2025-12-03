import streamlit as st
import pandas as pd
import numpy as np
import os
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

st.title("データ可視化ダッシュボード")

# データフォルダのパス
data_folder = Path("data2/output")

# データファイルの読み込み
@st.cache_data
def load_data_files():
    """data2/outputフォルダからCSVファイルを読み込む"""
    data_files = {}
    
    if data_folder.exists():
        for file_path in data_folder.glob("*.csv"):
            try:
                # 複数のエンコーディングを試す
                for encoding in ["utf-8-sig", "cp932", "shift_jis", "utf-8"]:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        data_files[file_path.name] = df
                        break
                    except UnicodeDecodeError:
                        continue
            except Exception as e:
                st.error(f"ファイル {file_path.name} の読み込みに失敗しました: {e}")
    
    return data_files

# データを読み込み
data_files = load_data_files()

if not data_files:
    st.warning("data2/outputフォルダにCSVファイルが見つかりません。")
    st.info("ダミーデータを表示します。")
    
    # ダミーデータ生成
    data = pd.DataFrame(
        np.random.randn(50, 3),
        columns=['A', 'B', 'C']
    )
    
    # 表の表示
    st.write("データサンプル:", data.head())
    
    # 折れ線グラフ
    st.line_chart(data)
    
    # ヒストグラム
    st.bar_chart(data)
else:
    # ファイル選択
    selected_file = st.selectbox("表示するファイルを選択してください:", list(data_files.keys()))
    
    if selected_file:
        data = data_files[selected_file]
        
        # ファイル情報
        st.subheader(f"ファイル: {selected_file}")
        st.write(f"行数: {len(data)}, 列数: {len(data.columns)}")
        
        # データプレビュー
        st.subheader("データプレビュー")
        st.dataframe(data.head(10))
        
        # 数値列の可視化
        numeric_cols = data.select_dtypes(include=[np.number]).columns.tolist()
        
        if numeric_cols:
            st.subheader("数値データの可視化")
            
            # 折れ線グラフ
            if len(numeric_cols) > 0:
                st.write("折れ線グラフ:")
                
                chart_data = data[numeric_cols]
                
                # データの最小値と最大値を取得
                min_val = chart_data.min().min()
                max_val = chart_data.max().max()
                
                # 縦軸の範囲調整オプション
                col1, col2 = st.columns(2)
                
                with col1:
                    use_auto_range = st.checkbox("自動範囲調整", value=True, help="データの範囲に基づいて自動で縦軸を調整")
                
                if use_auto_range:
                    # 自動範囲調整
                    y_min = min_val * 0.95
                    y_max = max_val * 1.05
                else:
                    # 手動範囲調整
                    with col2:
                        st.write("縦軸の範囲を手動で設定:")
                        y_min = st.number_input("最小値", value=float(min_val), format="%.2f")
                        y_max = st.number_input("最大値", value=float(max_val), format="%.2f")
                
                # 横軸の選択
                x_axis_options = ["インデックス"] + [col for col in data.columns if col not in numeric_cols]
                x_axis_col = st.selectbox("横軸を選択してください:", x_axis_options, index=0)
                
                # 横軸のデータを準備
                if x_axis_col == "インデックス":
                    x_data = chart_data.index
                    x_title = "インデックス"
                else:
                    x_data = data[x_axis_col]
                    x_title = x_axis_col
                
                # Plotlyを使用して折れ線グラフを作成
                fig = go.Figure()
                
                for col in numeric_cols:
                    # 長いラベルを短縮表示（レジェンド用）
                    if len(col) > 15:  # 15文字を超える場合は短縮
                        display_name = col[:12] + "..."  # 最初の12文字 + "..."
                    else:
                        display_name = col
                    
                    fig.add_trace(go.Scatter(
                        x=x_data,
                        y=chart_data[col],
                        mode='lines+markers',
                        name=display_name,
                        line=dict(width=2),
                        hovertemplate=f'<b>{col}</b><br>{x_title}: %{{x}}<br>値: %{{y}}<extra></extra>',
                        # カスタムデータで完全なラベル名を保持
                        customdata=[col] * len(chart_data)
                    ))
                
                # レイアウトを設定
                fig.update_layout(
                    title="数値データの折れ線グラフ",
                    xaxis_title=x_title,  # 動的に横軸タイトルを設定
                    yaxis_title="値",
                    yaxis=dict(range=[y_min, y_max]),
                    hovermode='x unified',
                    width=1000,  # 幅を広げる
                    height=600,  # 高さを増やす
                    # ラベルの見切れを防ぐ設定
                    legend=dict(
                        orientation="v",  # 縦方向にレジェンドを配置
                        yanchor="top",
                        y=1,
                        xanchor="left",
                        x=1.02,  # グラフの右側に配置
                        font=dict(size=9),  # フォントサイズを小さく
                        bgcolor="rgba(255,255,255,0.8)",  # 背景色を設定
                        bordercolor="rgba(0,0,0,0.2)",
                        borderwidth=1
                    ),
                    # マージンを大幅に調整してラベル用のスペースを確保
                    margin=dict(l=50, r=200, t=80, b=50),  # 右側のマージンを大幅に増やす
                    # タイトルの位置を調整
                    title_x=0.5,
                    title_font_size=16,
                    # レイアウトの自動調整を無効化
                    autosize=False
                )
                
                # グラフを表示
                st.plotly_chart(fig, use_container_width=True)
                
                # ラベル一覧を表示（長いラベル名の確認用）
                with st.expander("📋 データ列の詳細情報", expanded=False):
                    st.write("**数値列の一覧:**")
                    for i, col in enumerate(numeric_cols, 1):
                        st.write(f"{i}. **{col}**")
                        if len(col) > 15:
                            st.caption(f"   → レジェンドでは「{col[:12]}...」と表示")
                
                # データの統計情報を表示
                st.write("**データ範囲:**", f"最小値: {min_val:.2f}, 最大値: {max_val:.2f}")
            
            # 棒グラフ
            if len(numeric_cols) > 0:
                st.write("棒グラフ:")
                
                # 棒グラフ用の横軸選択
                bar_x_axis_options = ["インデックス"] + [col for col in data.columns if col not in numeric_cols]
                bar_x_axis_col = st.selectbox("棒グラフの横軸を選択してください:", bar_x_axis_options, index=0, key="bar_x_axis")
                
                # 横軸のデータを準備
                if bar_x_axis_col == "インデックス":
                    bar_x_data = data.index
                    bar_x_title = "インデックス"
                else:
                    bar_x_data = data[bar_x_axis_col]
                    bar_x_title = bar_x_axis_col
                
                # 棒グラフの表示方法を選択
                bar_chart_type = st.radio("棒グラフの表示方法:", ["グループ化", "積み上げ"], key="bar_chart_type")
                
                # Plotlyを使用して棒グラフを作成
                bar_fig = go.Figure()
                
                if bar_chart_type == "グループ化":
                    # グループ化された棒グラフ
                    for col in numeric_cols:
                        # 長いラベルを短縮表示
                        if len(col) > 15:
                            display_name = col[:12] + "..."
                        else:
                            display_name = col
                        
                        bar_fig.add_trace(go.Bar(
                            x=bar_x_data,
                            y=data[col],
                            name=display_name,
                            hovertemplate=f'<b>{col}</b><br>{bar_x_title}: %{{x}}<br>値: %{{y}}<extra></extra>'
                        ))
                else:
                    # 積み上げ棒グラフ
                    for col in numeric_cols:
                        # 長いラベルを短縮表示
                        if len(col) > 15:
                            display_name = col[:12] + "..."
                        else:
                            display_name = col
                        
                        bar_fig.add_trace(go.Bar(
                            x=bar_x_data,
                            y=data[col],
                            name=display_name,
                            hovertemplate=f'<b>{col}</b><br>{bar_x_title}: %{{x}}<br>値: %{{y}}<extra></extra>'
                        ))
                
                # 棒グラフのレイアウトを設定
                bar_fig.update_layout(
                    title="数値データの棒グラフ",
                    xaxis_title=bar_x_title,
                    yaxis_title="値",
                    barmode='group' if bar_chart_type == "グループ化" else 'stack',
                    hovermode='x unified',
                    width=1000,
                    height=500,
                    # ラベルの見切れを防ぐ設定
                    legend=dict(
                        orientation="v",
                        yanchor="top",
                        y=1,
                        xanchor="left",
                        x=1.02,
                        font=dict(size=9),
                        bgcolor="rgba(255,255,255,0.8)",
                        bordercolor="rgba(0,0,0,0.2)",
                        borderwidth=1
                    ),
                    margin=dict(l=50, r=200, t=80, b=50),
                    title_x=0.5,
                    title_font_size=16,
                    autosize=False
                )
                
                # 棒グラフを表示
                st.plotly_chart(bar_fig, use_container_width=True)
            
            # 統計情報
            st.subheader("統計情報")
            st.dataframe(data[numeric_cols].describe())
        else:
            st.warning("数値列が見つかりませんでした。")
        
        # カテゴリ列の可視化
        categorical_cols = data.select_dtypes(include=['object']).columns.tolist()
        
        if categorical_cols:
            st.subheader("カテゴリデータの可視化")
            
            for col in categorical_cols:
                st.write(f"**{col}** の値の分布:")
                value_counts = data[col].value_counts()
                st.bar_chart(value_counts)