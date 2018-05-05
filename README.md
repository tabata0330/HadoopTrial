# HadoopTrial
象本第3版の各章にあるコードをまとめたものです。読みながらアップデートしているので途中です。

# 各ソースコードを動かすには
**Hadoopの導入は前提としています。Macなら、[ここ](https://qiita.com/ysk_1031/items/26752b5da1629c9db8f7)を参照してください。**

まずは各プロジェクトをダウンロードするかリポジトリごとcloneしてください。方法は以下です。
* 各プロジェクトごとのダウンロード

`svn export https://github.com/tabata0330/HadoopTrial/branches/master/[dir_name]`

* リポジトリごとclone

`git clone https://github.com/tabata0330/HadoopTrial.git`

Gradleプロジェクトにしているのでgradleを導入してからビルドしてください。
* gradleの導入

```
brew update
brew install gradle
```
gradleが導入できたらプロジェクトまで移動してビルドします。`build.gradle`はそのままでビルドできると思います。
* プロジェクトのビルド

```
cd [project_dir]
gradle build
```
ビルドしたらjarに少し手を加えないと

`エラー: メイン・クラスjarが見つからなかったかロードできませんでした`

だったり

`Mkdirs failed to create /[path]/META-INF/license`

というエラーが出てしまうことがあります。そんな時は

```
zip -d [jar_name] META-INF/LICENSE
jar tvf [jar_name] | grep -i license
```
で直ります。(原因の解明できてないのでわかる人いたら教えて欲しいです)

これを実行したらhadoopコマンドを使って実行しましょう。
* hadoopコマンドを用いた実行

`hadoop jar [jar_file] <args>*`

以上で実行可能かと思います。(2018/05/04)

