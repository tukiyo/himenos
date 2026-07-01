package main

import (
	"bytes"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type ScriptInfo struct {
	Name    string
	ModTime string
}

type PageData struct {
	Scripts        []ScriptInfo
	SelectedScript string
	SearchQuery    string
	Argument       string
	Output         string
	FileContent    string
	CurrentTab     string // "run", "edit", "delete"
}

const htmlTemplate = `<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>Git連動シェル管理パネル</title>
    <style>
        body { font-family: monospace; background: #1e1e2e; color: #cdd6f4; margin: 0; padding: 10px; }
        .container { display: flex; }
        .sidebar { width: 30%; border-right: 1px solid #45475a; padding-right: 10px; }
        .main-content { width: 70%; padding-left: 15px; }
        .search-box { width: 100%; padding: 5px; background: #313244; border: 1px solid #45475a; color: #cdd6f4; margin-bottom: 10px; box-sizing: border-box; }
        .script-item { display: block; padding: 6px; color: #89b4fa; text-decoration: none; border-bottom: 1px solid #313244; }
        .script-item:hover, .active { background: #45475a; color: #a6e3a1; }
        .time-text { font-size: 0.8em; color: #6c7086; display: block; }
        input[type="text"], textarea, button { padding: 5px; background: #313244; border: 1px solid #45475a; color: #cdd6f4; font-family: monospace; }
        button { background: #a6e3a1; color: #11111b; font-weight: bold; cursor: pointer; }
        .btn-danger { background: #f38ba8; color: #11111b; }
        .btn-new { background: #fab387; color: #11111b; width: 100%; padding: 8px; margin-bottom: 15px; text-align: center; display: block; text-decoration: none; font-weight: bold; box-sizing: border-box; }
        pre { background: #11111b; padding: 10px; color: #a6e3a1; white-space: pre-wrap; word-wrap: break-word; border: 1px solid #313244; }
        .tabs { margin-bottom: 15px; border-bottom: 1px solid #45475a; padding-bottom: 5px; }
        .tab-link { padding: 5px 10px; color: #cdd6f4; text-decoration: none; margin-right: 5px; background: #313244; }
        .tab-active { background: #a6e3a1; color: #11111b; font-weight: bold; }
    </style>
</head>
<body>
    <h2>⚡ Git連動スクリプト自動検知・管理パネル</h2>
    <div class="container">
        <div class="sidebar">
            <a href="/?tab=new&q={{.SearchQuery}}" class="btn-new">＋ 新規スクリプト作成</a>
            <form action="/" method="GET">
                <input type="text" name="q" class="search-box" placeholder="絞り込み検索..." value="{{.SearchQuery}}" id="js-search">
                <noscript><button type="submit">検索</button></noscript>
            </form>
            <div id="js-list">
                {{range .Scripts}}
                <a href="/?s={{.Name}}&q={{$.SearchQuery}}" class="script-item {{if eq .Name $.SelectedScript}}active{{end}}">
                    <span>{{.Name}}</span>
                    <span class="time-text">🕒 {{.ModTime}}</span>
                </a>
                {{else}}
                <div>なし</div>
                {{end}}
            </div>
        </div>

        <div class="main-content">
            {{if eq .CurrentTab "new"}}
                <h3>📄 新規スクリプト作成</h3>
                <form action="/action" method="POST">
                    <input type="hidden" name="action" value="create">
                    <p><label>ファイル名 (例: test.sh):<br>
                    <input type="text" name="filename" style="width: 50%;" required></label></p>
                    <p><label>スクリプト内容:<br>
                    <textarea name="content" rows="15" style="width: 100%;" required>#!/bin/sh&#10;echo "Hello"</textarea></label></p>
                    <button type="submit">作成 & Git Commit</button>
                </form>
            {{else if .SelectedScript}}
                <h3>選択中: <span style="color:#a6e3a1;">{{.SelectedScript}}</span></h3>
                
                <div class="tabs">
                    <a href="/?s={{.SelectedScript}}&tab=run&q={{.SearchQuery}}" class="tab-link {{if eq .CurrentTab "run"}}tab-active{{end}}">[実行]</a>
                    <a href="/?s={{.SelectedScript}}&tab=edit&q={{.SearchQuery}}" class="tab-link {{if eq .CurrentTab "edit"}}tab-active{{end}}">[編集]</a>
                    <a href="/?s={{.SelectedScript}}&tab=delete&q={{.SearchQuery}}" class="tab-link {{if eq .CurrentTab "delete"}}tab-active{{end}}">[削除]</a>
                </div>

                {{if eq .CurrentTab "run"}}
                    <form action="/action" method="POST">
                        <input type="hidden" name="action" value="run">
                        <input type="hidden" name="s" value="{{.SelectedScript}}">
                        <label>引数: </label>
                        <input type="text" name="arg" value="{{.Argument}}" style="width:60%;">
                        <button type="submit">実行する</button>
                    </form>
                {{else if eq .CurrentTab "edit"}}
                    <form action="/action" method="POST">
                        <input type="hidden" name="action" value="update">
                        <input type="hidden" name="s" value="{{.SelectedScript}}">
                        <p><textarea name="content" rows="15" style="width: 100%;">{{.FileContent}}</textarea></p>
                        <button type="submit">変更を保存 & Git Commit</button>
                    </form>
                {{else if eq .CurrentTab "delete"}}
                    <div style="background:#513535; padding: 15px; border: 1px solid #f38ba8; border-radius:5px;">
                        <p>本当にこのスクリプトを削除しますか？ (Git上の履歴は残ります)</p>
                        <form action="/action" method="POST">
                            <input type="hidden" name="action" value="delete">
                            <input type="hidden" name="s" value="{{.SelectedScript}}">
                            <button type="submit" class="btn-danger">完全に削除 & Git Commit</button>
                        </form>
                    </div>
                {{end}}
            {{else}}
                <p>左側からスクリプトを選択するか、新規作成してください。</p>
            {{end}}

            <h4>📋 ログ / 実行結果:</h4>
            <pre>{{if .Output}}{{.Output}}{{else}}ここに結果が表示されます{{end}}</pre>
        </div>
    </div>

    <script>
        // JS対応ブラウザ用リアルタイム検索
        const searchInput = document.getElementById('js-search');
        if(searchInput) {
            searchInput.addEventListener('input', function(e) {
                const query = e.target.value.toLowerCase();
                document.querySelectorAll('#js-list .script-item').forEach(item => {
                    const text = item.querySelector('span').textContent.toLowerCase();
                    if (text.includes(query)) {
                        item.style.display = 'block';
                        const url = new URL(item.href, window.location.origin);
                        url.searchParams.set('q', e.target.value);
                        item.href = url.pathname + url.search;
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        }
    </script>
</body>
</html>`

// Gitにコミットする関数
func gitCommit(message string, file string) string {
	exec.Command("git", "-C", "/app/scripts", "add", file).Run()
	cmd := exec.Command("git", "-C", "/app/scripts", "commit", "-m", message)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	_ = cmd.Run()
	return out.String()
}

func main() {
	tmpl := template.Must(template.New("index").Parse(htmlTemplate))
	scriptsDir := "/app/scripts"

	// 起動時にリポジトリを初期化
	if _, err := os.Stat(filepath.Join(scriptsDir, ".git")); os.IsNotExist(err) {
		exec.Command("git", "-C", scriptsDir, "init").Run()
		exec.Command("git", "-C", scriptsDir, "config", "user.name", "WebPanel").Run()
		exec.Command("git", "-C", scriptsDir, "config", "user.email", "webpanel@local").Run()
	}

	// メインの表示処理
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		searchQuery := r.URL.Query().Get("q")
		selected := r.URL.Query().Get("s")
		tab := r.URL.Query().Get("tab")
		if tab == "" {
			tab = "run"
		}

		files, _ := os.ReadDir(scriptsDir)
		var scripts []ScriptInfo
		for _, f := range files {
			if !f.IsDir() && strings.HasSuffix(f.Name(), ".sh") {
				if searchQuery == "" || strings.Contains(strings.ToLower(f.Name()), strings.ToLower(searchQuery)) {
					info, _ := f.Info()
					modTime := info.ModTime().Format("2006-01-02 15:04:05")
					scripts = append(scripts, ScriptInfo{Name: f.Name(), ModTime: modTime})
				}
			}
		}

		data := PageData{
			Scripts:        scripts,
			SelectedScript: selected,
			SearchQuery:    searchQuery,
			CurrentTab:     tab,
			Output:         r.URL.Query().Get("out"),
		}

		if tab == "new" {
			data.CurrentTab = "new"
			data.SelectedScript = ""
		}

		// 編集タブの場合はファイルの中身を読み込む
		if selected != "" && tab == "edit" {
			content, _ := os.ReadFile(filepath.Join(scriptsDir, filepath.Base(selected)))
			data.FileContent = string(content)
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = tmpl.Execute(w, data)
	})

	// 処理(POST)を受け付けるハンドラ
	http.HandleFunc("/action", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		action := r.FormValue("action")
		selected := r.FormValue("s")
		q := r.FormValue("q")
		var output string
		var redirectTo string

		switch action {
		case "run":
			arg := r.FormValue("arg")
			safeScript := filepath.Base(selected)
			scriptPath := filepath.Join(scriptsDir, safeScript)

			cmd := exec.Command("/bin/sh", scriptPath, arg)
			var out bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = &out
			_ = cmd.Run()
			output = out.String()
			redirectTo = fmt.Sprintf("/?s=%s&tab=run&q=%s&out=%s", selected, q, r.FormValue("arg")+"\n\n"+output)

		case "create":
			filename := filepath.Base(r.FormValue("filename"))
			if !strings.HasSuffix(filename, ".sh") {
				filename += ".sh"
			}
			content := r.FormValue("content")
			filePath := filepath.Join(scriptsDir, filename)

			_ = os.WriteFile(filePath, []byte(content), 0755)
			gitOut := gitCommit("Web: Create "+filename, filename)
			redirectTo = fmt.Sprintf("/?s=%s&tab=run&out=%s", filename, "【Git Log】\n"+gitOut)

		case "update":
			safeScript := filepath.Base(selected)
			content := r.FormValue("content")
			filePath := filepath.Join(scriptsDir, safeScript)

			_ = os.WriteFile(filePath, []byte(content), 0755)
			gitOut := gitCommit("Web: Update "+safeScript, safeScript)
			redirectTo = fmt.Sprintf("/?s=%s&tab=edit&q=%s&out=%s", selected, q, "【Git Log】\n"+gitOut)

		case "delete":
			safeScript := filepath.Base(selected)
			filePath := filepath.Join(scriptsDir, safeScript)

			_ = os.Remove(filePath)
			// 削除をGitに記録
			exec.Command("git", "-C", scriptsDir, "rm", safeScript).Run()
			cmd := exec.Command("git", "-C", scriptsDir, "commit", "-m", "Web: Delete "+safeScript)
			var out bytes.Buffer
			cmd.Stdout = &out
			_ = cmd.Run()
			redirectTo = fmt.Sprintf("/?out=%s", "【Git Log】\n"+out.String())
		}

		http.Redirect(w, r, redirectTo, http.StatusSeeOther)
	})

	fmt.Println("Server started on :8080")
	_ = http.ListenAndServe(":8080", nil)
}
