package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// 設定ファイルパス
// 設定ファイルパス
// 設定ファイルパス
const (
	jobsFile      = "jobs.yaml"
	schedulesFile = "schedules.yaml"
	settingsFile  = "settings.yaml"
	historyFile   = "history.yaml"
	nodesFile     = "nodes.yaml"
	monitorsFile  = "monitors.yaml"
	monHistoryFile= "monitoring_history.yaml"
	scriptsDir    = "./scripts"
)

// --- データモデルの定義 ---

type Settings struct {
	SMTPHost      string `yaml:"smtp_host"`
	SMTPPort      string `yaml:"smtp_port"`
	SMTPUser      string `yaml:"smtp_user"`
	SMTPPass      string `yaml:"smtp_pass"`
	SMTPFrom      string `yaml:"smtp_from"`
	SMTPTo        string `yaml:"smtp_to"`
	SlackWebhook  string `yaml:"slack_webhook"`
	DefaultNotify string `yaml:"default_notify"` // デフォルト通知設定
}

type JobType string

const (
	TypeUnit JobType = "unit"
	TypeNet  JobType = "net"
	TypeJob  JobType = "job"
)

type JobNode struct {
	ID             string          `yaml:"id"`
	Name           string          `yaml:"name"`
	Type           JobType         `yaml:"type"`
	Description    string          `yaml:"description"`
	ParentID       string          `yaml:"parent_id"` // 親ノードID
	OriginalParentID string          `yaml:"original_parent_id"` // ゴミ箱から元に戻す用
	Children       []string        `yaml:"children"`  // 子ノードIDリスト
	Command        string          `yaml:"command"`
	StopCommand    string          `json:"stop_command"`
	RunUser        string          `yaml:"run_user"`
	WaitConditions []WaitCondition `yaml:"wait_conditions"`
	WaitRelation   string          `yaml:"wait_relation"`   // "AND" or "OR"
	EndOnWaitFail  bool            `json:"end_on_wait_fail"` // 条件不一致時に終了するか
	WaitFailValue  int             `json:"wait_fail_value"`
	CalendarID     string          `json:"calendar_id"`
	CalendarVal    int             `json:"calendar_val"`
	IsHold         bool            `json:"is_hold"`
	IsSkip         bool            `json:"is_skip"`
	SkipValue      int             `json:"skip_value"`
	NormalRange    string          `yaml:"normal_range"` // 例: "0" または "0-0"
	WarnRange      string          `yaml:"warn_range"`   // 例: "1-5"
	NotifyStart    string          `yaml:"notify_start"` // "slack", "email", "both", ""
	NotifyNormal   string          `yaml:"notify_normal"`
	NotifyWarning  string          `yaml:"notify_warning"`
	NotifyError    string          `yaml:"notify_error"`
}

type WaitCondition struct {
	Type  string `yaml:"type"`   // "status" or "value"
	JobID string `yaml:"job_id"` // 先行ジョブID
	Value string `yaml:"value"`  // "正常" / "警告" / "異常" または終了値
}

type Schedule struct {
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	JobID    string `yaml:"job_id"` // 対象ジョブユニットID
	Type     string `yaml:"type"`   // "datetime" or "weekly" or "daily" or "hourly" or "interval" or "cron"
	Month    string `yaml:"month"`   // "1"-"12" or "*"
	Day      string `yaml:"day"`     // "1"-"31" or "*"
	Weekday  string `yaml:"weekday"` // "0"-"6" or "*" (0=日曜日)
	Hour     string `yaml:"hour"`    // "0"-"23"
	Minute   string `yaml:"minute"`  // "0"-"59"
	Interval int    `yaml:"interval"` // 実行間隔 (分)
	CronExpr string `yaml:"cron_expr"` // crontab形式式
	Enabled  bool   `yaml:"enabled"`
}

type JobSession struct {
	SessionID   string                `yaml:"session_id"`   // 時刻ベース (20060102150405-000)
	UnitID      string                `yaml:"unit_id"`
	UnitName    string                `yaml:"unit_name"`
	TriggerType string                `yaml:"trigger_type"` // "スケジュール", "手動実行"
	TriggerInfo string                `yaml:"trigger_info"` // スケジュール名 or ユーザー名
	StartDate   string                `yaml:"start_date"`
	EndDate     string                `yaml:"end_date"`
	Status      string                `yaml:"status"` // "実行中", "正常終了", "警告終了", "異常終了", "中断"
	ExitValue   int                   `yaml:"exit_value"`
	Nodes       map[string]*NodeState `yaml:"nodes"`
}

type NodeState struct {
	JobID     string `yaml:"job_id"`
	Name      string `yaml:"name"`
	Type      string `yaml:"type"`
	Status    string `yaml:"status"` // "待機中", "保留中", "スキップ", "実行中", "停止処理中", "中断", "コマンド停止", "終了", "起動失敗"
	ExitValue int    `yaml:"exit_value"`
	StartDate string `yaml:"start_date"`
	EndDate   string `yaml:"end_date"`
	Log       string `yaml:"log"`
	PID       int    `json:"pid"`
}

type ManagedNode struct {
	ID          string `yaml:"id"`
	Name        string `yaml:"name"`
	Platform    string `yaml:"platform"` // "LINUX", "WINDOWS", "OTHER"
	IPAddress   string `yaml:"ip_address"`
	Description string `yaml:"description"`
}

type MonitorSetting struct {
	ID         string `yaml:"id"`
	Name       string `yaml:"name"`
	NodeID     string `yaml:"node_id"`
	Type       string `yaml:"type"`       // "ping", "http", "port"
	Target     string `yaml:"target"`     // http: URL, port: port番号
	Enabled    bool   `yaml:"enabled"`
	LastStatus string `yaml:"last_status"` // "正常", "異常", "未実施"
	LastCheck  string `yaml:"last_check"`
}

type MonitorHistory struct {
	CheckTime string `yaml:"check_time"`
	MonitorID string `yaml:"monitor_id"`
	Name      string `yaml:"name"`
	Type      string `yaml:"type"`
	NodeName  string `yaml:"node_name"`
	Status    string `yaml:"status"` // "正常", "異常"
	Log       string `yaml:"log"`
}

type BackupData struct {
	Jobs      []*JobNode        `yaml:"jobs"`
	Schedules []Schedule        `yaml:"schedules"`
	Settings  Settings          `yaml:"settings"`
	Nodes     []*ManagedNode    `yaml:"nodes"`
	Monitors  []*MonitorSetting `yaml:"monitors"`
}

func matchCronField(field string, value int) bool {
	if field == "*" || field == "" {
		return true
	}
	if strings.Contains(field, ",") {
		parts := strings.Split(field, ",")
		for _, p := range parts {
			if matchCronField(p, value) {
				return true
			}
		}
		return false
	}
	if strings.HasPrefix(field, "*/") {
		step, err := strconv.Atoi(field[2:])
		if err != nil {
			return false
		}
		return value%step == 0
	}
	if strings.Contains(field, "-") {
		parts := strings.Split(field, "-")
		if len(parts) == 2 {
			start, _ := strconv.Atoi(parts[0])
			end, _ := strconv.Atoi(parts[1])
			return value >= start && value <= end
		}
		return false
	}
	num, err := strconv.Atoi(field)
	if err != nil {
		return false
	}
	return num == value
}

func matchCronSchedule(expr string, t time.Time) bool {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return false
	}
	min := t.Minute()
	hour := t.Hour()
	dom := t.Day()
	mon := int(t.Month())
	dow := int(t.Weekday())
	return matchCronField(fields[0], min) &&
		matchCronField(fields[1], hour) &&
		matchCronField(fields[2], dom) &&
		matchCronField(fields[3], mon) &&
		matchCronField(fields[4], dow)
}



// --- グローバル変数 & エンジン定義 ---

type Engine struct {
	mu           sync.Mutex
	sessions     []*JobSession
	activeCmds   map[string]*exec.Cmd      // key: "session_id:job_id"
	stateChanged map[string]chan bool      // key: session_id
	settings     Settings
	jobs         map[string]*JobNode
	schedules    []Schedule
	nodes        map[string]*ManagedNode
	monitors     map[string]*MonitorSetting
	monHistory   []MonitorHistory
}

var engine *Engine

func initEngine() {
	engine = &Engine{
		activeCmds:   make(map[string]*exec.Cmd),
		stateChanged: make(map[string]chan bool),
		jobs:         make(map[string]*JobNode),
	}
	engine.loadAll()
}

// --- ファイル永続化とGitコミット ---

func (e *Engine) loadAll() {
	e.mu.Lock()
	defer e.mu.Unlock()

	_ = os.MkdirAll(scriptsDir, 0755)

	// Settings
	if data, err := os.ReadFile(settingsFile); err == nil {
		_ = yaml.Unmarshal(data, &e.settings)
	}
	// Jobs
	var jobList []*JobNode
	if data, err := os.ReadFile(jobsFile); err == nil {
		if err := yaml.Unmarshal(data, &jobList); err == nil {
			e.jobs = make(map[string]*JobNode)
			for _, j := range jobList {
				e.jobs[j.ID] = j
			}
		}
	}
	// Schedules
	if data, err := os.ReadFile(schedulesFile); err == nil {
		_ = yaml.Unmarshal(data, &e.schedules)
	}
	// History
	if data, err := os.ReadFile(historyFile); err == nil {
		_ = yaml.Unmarshal(data, &e.sessions)
	}
	// Nodes
	var nodeList []*ManagedNode
	e.nodes = make(map[string]*ManagedNode)
	if data, err := os.ReadFile(nodesFile); err == nil {
		if err := yaml.Unmarshal(data, &nodeList); err == nil {
			for _, n := range nodeList {
				e.nodes[n.ID] = n
			}
		}
	}
	// Monitors
	var monitorList []*MonitorSetting
	e.monitors = make(map[string]*MonitorSetting)
	if data, err := os.ReadFile(monitorsFile); err == nil {
		if err := yaml.Unmarshal(data, &monitorList); err == nil {
			for _, m := range monitorList {
				e.monitors[m.ID] = m
			}
		}
	}
	// Monitor History
	if data, err := os.ReadFile(monHistoryFile); err == nil {
		_ = yaml.Unmarshal(data, &e.monHistory)
	}
}

func (e *Engine) saveJobs() {
	var list []*JobNode
	for _, j := range e.jobs {
		list = append(list, j)
	}
	data, _ := yaml.Marshal(list)
	_ = os.WriteFile(jobsFile, data, 0644)
	gitCommit("System: Update jobs", jobsFile)
}

func (e *Engine) saveSchedules() {
	data, _ := yaml.Marshal(e.schedules)
	_ = os.WriteFile(schedulesFile, data, 0644)
	gitCommit("System: Update schedules", schedulesFile)
}

func (e *Engine) saveSettings() {
	data, _ := json.MarshalIndent(e.settings, "", "  ")
	_ = os.WriteFile(settingsFile, data, 0644)
	gitCommit("System: Update settings", settingsFile)
}

func (e *Engine) saveNodes() {
	var list []*ManagedNode
	for _, n := range e.nodes {
		list = append(list, n)
	}
	data, _ := yaml.Marshal(list)
	_ = os.WriteFile(nodesFile, data, 0644)
	gitCommit("System: Update nodes", nodesFile)
}

func (e *Engine) saveMonitors() {
	var list []*MonitorSetting
	for _, m := range e.monitors {
		list = append(list, m)
	}
	data, _ := yaml.Marshal(list)
	_ = os.WriteFile(monitorsFile, data, 0644)
	gitCommit("System: Update monitors", monitorsFile)
}

func (e *Engine) saveMonHistory() {
	data, _ := yaml.Marshal(e.monHistory)
	_ = os.WriteFile(monHistoryFile, data, 0644)
}

func (e *Engine) saveHistory() {
	data, _ := yaml.Marshal(e.sessions)
	_ = os.WriteFile(historyFile, data, 0644)
	// 履歴は頻繁に更新されるため、Gitへのコミットは省略するか、セッション終了時のみ行うようにします。
}

func gitCommit(message string, file string) {
	go func() {
		// ローカルGit連携
		if _, err := os.Stat(".git"); os.IsNotExist(err) {
			exec.Command("git", "init").Run()
			exec.Command("git", "config", "user.name", "HimenosGo").Run()
			exec.Command("git", "config", "user.email", "himenosgo@local").Run()
		}
		_ = exec.Command("git", "add", file).Run()
		_ = exec.Command("git", "commit", "-m", message).Run()
	}()
}

// --- 通知機能の実装 ---

func sendSlack(webhookURL string, text string) {
	if webhookURL == "" {
		return
	}
	payload, _ := json.Marshal(map[string]string{"text": text})
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
	if err == nil {
		_ = resp.Body.Close()
	}
}

func sendEmail(settings Settings, subject, body string) {
	if settings.SMTPHost == "" || settings.SMTPTo == "" {
		// SMTP未設定の場合は標準出力とログにモック出力
		fmt.Printf("[Email Notification Mock]\nSubject: %s\nTo: %s\nBody:\n%s\n-------------------------\n", subject, settings.SMTPTo, body)
		return
	}
	msg := []byte(fmt.Sprintf("To: %s\r\nSubject: %s\r\n\r\n%s", settings.SMTPTo, subject, body))
	auth := smtp.PlainAuth("", settings.SMTPUser, settings.SMTPPass, settings.SMTPHost)
	addr := fmt.Sprintf("%s:%s", settings.SMTPHost, settings.SMTPPort)
	_ = smtp.SendMail(addr, auth, settings.SMTPFrom, []string{settings.SMTPTo}, msg)
}

func (e *Engine) notify(node *JobNode, trigger string, sessionID string, status string, exitVal int) {
	notifyType := node.NotifyNormal // ジョブ個別の設定 (Normalに保存)
	if notifyType == "" || notifyType == "default" {
		notifyType = e.settings.DefaultNotify
	}
	if notifyType == "none" || notifyType == "" {
		return
	}

	subject := fmt.Sprintf("[%s] Job %s %s (%s)", status, node.Name, trigger, sessionID)
	body := fmt.Sprintf("Job Name: %s\nType: %s\nSession ID: %s\nStatus: %s\nExit Value: %d\nTime: %s\n",
		node.Name, node.Type, sessionID, status, exitVal, time.Now().Format("2006-01-02 15:04:05"))

	if notifyType == "slack" || notifyType == "both" {
		go sendSlack(e.settings.SlackWebhook, fmt.Sprintf("*%s*\n%s", subject, body))
	}
	if notifyType == "email" || notifyType == "both" {
		go sendEmail(e.settings, subject, body)
	}
}

// --- ジョブエンジンのコアロジック ---

func (e *Engine) StartSession(unitID string, triggerType string, triggerInfo string) string {
	e.mu.Lock()
	unit, exists := e.jobs[unitID]
	if !exists {
		e.mu.Unlock()
		fmt.Printf("[Engine Error] StartSession failed: Job '%s' not found.\n", unitID)
		return ""
	}

	sessionID := time.Now().Format("20060102150405") + fmt.Sprintf("-%03d", time.Now().Nanosecond()/1000000%1000)
	session := &JobSession{
		SessionID:   sessionID,
		UnitID:      unitID,
		UnitName:    unit.Name,
		TriggerType: triggerType,
		TriggerInfo: triggerInfo,
		StartDate:   time.Now().Format("2006-01-02 15:04:05"),
		Status:      "実行中",
		Nodes:       make(map[string]*NodeState),
	}

	// セッション内のノード状態を構築
	var collectNodes func(id string)
	collectNodes = func(id string) {
		n, exists := e.jobs[id]
		if !exists {
			return
		}
		status := "待機中"
		if n.IsHold {
			status = "保留中"
		}
		session.Nodes[id] = &NodeState{
			JobID:  id,
			Name:   n.Name,
			Type:   string(n.Type),
			Status: status,
		}
		for _, childID := range n.Children {
			collectNodes(childID)
		}
	}
	collectNodes(unitID)

	e.sessions = append([]*JobSession{session}, e.sessions...) // 最新を先頭に
	e.stateChanged[sessionID] = make(chan bool, 100)
	e.saveHistory()
	e.mu.Unlock()

	go e.runSession(sessionID)
	return sessionID
}

func (e *Engine) runSession(sessionID string) {
	stateChan := e.getStateChan(sessionID)
	if stateChan == nil {
		return
	}

	for {
		// セッションの終了判定と、実行可能ノードの探索
		session := e.getSession(sessionID)
		if session == nil || session.Status != "実行中" {
			break
		}

		e.mu.Lock()
		// 1. 各待機中のノードに対して、待ち条件の評価
		allFinished := true
		var runnableNodes []string

		for id, state := range session.Nodes {
			if state.Status == "待機中" {
				allFinished = false
				node := e.jobs[id]
				if node == nil {
					state.Status = "起動失敗"
					continue
				}

				if e.evaluateWaitConditions(session, node) {
					runnableNodes = append(runnableNodes, id)
				}
			} else if state.Status == "実行中" || state.Status == "保留中" || state.Status == "停止処理中" || state.Status == "中断" {
				allFinished = false
			}
		}
		e.mu.Unlock()

		// 2. 実行可能になったノードを並列起動
		for _, id := range runnableNodes {
			go e.executeNode(sessionID, id)
		}

		if allFinished {
			// セッション全体のステータス決定
			e.mu.Lock()
			unitState := session.Nodes[session.UnitID]
			session.EndDate = time.Now().Format("2006-01-02 15:04:05")
			if unitState != nil {
				session.ExitValue = unitState.ExitValue
				switch unitState.Status {
				case "終了":
					session.Status = e.determineStatusFromRange(e.jobs[session.UnitID], unitState.ExitValue)
				case "中断":
					session.Status = "中断"
				default:
					session.Status = "異常終了"
				}
			} else {
				session.Status = "異常終了"
			}
			e.saveHistory()
			e.mu.Unlock()
			break
		}

		// 次の状態変化まで待機
		select {
		case <-stateChan:
		case <-time.After(1 * time.Second): // セーフティポーリング
		}
	}

	e.cleanupSession(sessionID)
}

func (e *Engine) evaluateWaitConditions(session *JobSession, node *JobNode) bool {
	if len(node.WaitConditions) == 0 {
		return true
	}

	conditionsMet := 0
	for _, cond := range node.WaitConditions {
		priorState, exists := session.Nodes[cond.JobID]
		if !exists {
			continue // 存在しない先行ジョブは無視（エラー扱い）
		}

		// 先行ジョブが終端状態かどうかチェック
		isFinished := priorState.Status == "終了" || priorState.Status == "スキップ" || priorState.Status == "コマンド停止" || priorState.Status == "起動失敗"
		if !isFinished {
			continue
		}

		// 条件評価
		met := false
		if cond.Type == "status" {
			// 先行ジョブの終了状態の範囲から判定
			priorNode := e.jobs[cond.JobID]
			priorStatus := e.determineStatusFromRange(priorNode, priorState.ExitValue)
			if cond.Value == "正常" && priorStatus == "正常終了" {
				met = true
			} else if cond.Value == "警告" && priorStatus == "警告終了" {
				met = true
			} else if cond.Value == "異常" && priorStatus == "異常終了" {
				met = true
			}
		} else if cond.Type == "value" {
			val, _ := strconv.Atoi(cond.Value)
			if priorState.ExitValue == val {
				met = true
			}
		}

		if met {
			conditionsMet++
		}
	}

	if node.WaitRelation == "OR" {
		return conditionsMet > 0
	}
	return conditionsMet == len(node.WaitConditions)
}

func (e *Engine) determineStatusFromRange(node *JobNode, exitVal int) string {
	if node == nil {
		return "異常終了"
	}
	// 正常範囲判定
	if inRange(node.NormalRange, exitVal) {
		return "正常終了"
	}
	// 警告範囲判定
	if inRange(node.WarnRange, exitVal) {
		return "警告終了"
	}
	return "異常終了"
}

func inRange(rangeStr string, val int) bool {
	if rangeStr == "" {
		return false
	}
	parts := strings.Split(rangeStr, "-")
	if len(parts) == 1 {
		v, err := strconv.Atoi(parts[0])
		return err == nil && v == val
	} else if len(parts) == 2 {
		minV, err1 := strconv.Atoi(parts[0])
		maxV, err2 := strconv.Atoi(parts[1])
		return err1 == nil && err2 == nil && val >= minV && val <= maxV
	}
	return false
}

func (e *Engine) executeNode(sessionID string, jobID string) {
	session := e.getSession(sessionID)
	if session == nil {
		return
	}

	e.mu.Lock()
	state := session.Nodes[jobID]
	node := e.jobs[jobID]
	e.mu.Unlock()

	if state == nil || node == nil {
		return
	}

	// スキップの処理
	if node.IsSkip {
		e.mu.Lock()
		state.Status = "スキップ"
		state.ExitValue = node.SkipValue
		state.StartDate = time.Now().Format("2006-01-02 15:04:05")
		state.EndDate = state.StartDate
		state.Log = "スキップ設定により実行をスキップしました。"
		e.saveHistory()
		e.mu.Unlock()
		e.notifyStateChanged(sessionID)
		return
	}

	// 保留の処理
	if state.Status == "保留中" {
		// ユーザーの解除操作待ち。ポーリングするか、状態変化を待つ
		for {
			time.Sleep(500 * time.Millisecond)
			session = e.getSession(sessionID)
			if session == nil || session.Status != "実行中" {
				return
			}
			e.mu.Lock()
			curStatus := state.Status
			e.mu.Unlock()
			if curStatus != "保留中" {
				break
			}
		}
	}

	e.mu.Lock()
	state.Status = "実行中"
	state.StartDate = time.Now().Format("2006-01-02 15:04:05")
	e.saveHistory()
	e.mu.Unlock()
	e.notifyStateChanged(sessionID)

	e.notify(node, "start", sessionID, "実行中", 0)

	var exitCode int
	var logBuf bytes.Buffer

		if node.Type == TypeJob {
		// ジョブの実行（コマンドライン）
		scriptPath := node.Command
		argsStr := node.RunUser // RunUserフィールドを引数として再利用
		argsStr = e.replaceVariables(argsStr, session, node)

		cmdStr := scriptPath
		if argsStr != "" {
			cmdStr += " " + argsStr
		}

		var cmd *exec.Cmd
		if runtime.GOOS == "windows" {
			// Windowsの場合は拡張子が.batならそのまま実行、それ以外（.shなど）は git bash / sh があればそれを使うが、
			// シンプルに cmd /c を介してキック
			cmd = exec.Command("cmd.exe", "/c", cmdStr)
		} else {
			cmd = exec.Command("sh", "-c", cmdStr)
		}

		cmd.Stdout = &logBuf
		cmd.Stderr = &logBuf

		e.mu.Lock()
		e.activeCmds[sessionID+":"+jobID] = cmd
		e.mu.Unlock()

		err := cmd.Start()
		if err != nil {
			e.mu.Lock()
			state.Status = "起動失敗"
			state.EndDate = time.Now().Format("2006-01-02 15:04:05")
			state.Log = "プロセスの起動に失敗しました: " + err.Error()
			delete(e.activeCmds, sessionID+":"+jobID)
			e.saveHistory()
			e.mu.Unlock()
			e.notify(node, "error", sessionID, "起動失敗", 1)
			e.notifyStateChanged(sessionID)
			return
		}

		e.mu.Lock()
		state.PID = cmd.Process.Pid
		e.saveHistory()
		e.mu.Unlock()

		err = cmd.Wait()
		e.mu.Lock()
		delete(e.activeCmds, sessionID+":"+jobID)
		state.PID = 0

		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				exitCode = exitError.ExitCode()
			} else {
				exitCode = -1 // 不明なエラー
			}
		} else {
			exitCode = 0
		}
		e.mu.Unlock()

	} else if node.Type == TypeNet {
		// ジョブネットの実行（子ノードが終了するのを待つ）
		// 子ノードがすべて終了するまでポーリング
		for {
			time.Sleep(500 * time.Millisecond)
			session = e.getSession(sessionID)
			if session == nil || session.Status != "実行中" {
				return
			}

			e.mu.Lock()
			childrenFinished := true
			maxExitVal := 0
			hasError := false
			hasWarn := false

			for _, childID := range node.Children {
				cState := session.Nodes[childID]
				if cState == nil || (cState.Status != "終了" && cState.Status != "スキップ" && cState.Status != "コマンド停止" && cState.Status != "起動失敗") {
					childrenFinished = false
					break
				}
				if cState.ExitValue > maxExitVal {
					maxExitVal = cState.ExitValue
				}
				childNode := e.jobs[childID]
				childStatus := e.determineStatusFromRange(childNode, cState.ExitValue)
				if childStatus == "異常終了" || cState.Status == "起動失敗" {
					hasError = true
				} else if childStatus == "警告終了" {
					hasWarn = true
				}
			}
			e.mu.Unlock()

			if childrenFinished {
				exitCode = maxExitVal
				if hasError {
					logBuf.WriteString("子ジョブに異常終了があります。\n")
				} else if hasWarn {
					logBuf.WriteString("子ジョブに警告終了があります。\n")
				} else {
					logBuf.WriteString("すべての子ジョブが正常終了しました。\n")
				}
				break
			}
		}
	}

	// 終了ステータスの判定と更新
	e.mu.Lock()
	if state.Status != "コマンド停止" && state.Status != "中断" {
		state.Status = "終了"
		state.ExitValue = exitCode
		state.EndDate = time.Now().Format("2006-01-02 15:04:05")
		state.Log = logBuf.String()

		statusName := e.determineStatusFromRange(node, exitCode)
		if statusName == "正常終了" {
			e.notify(node, "normal", sessionID, "正常終了", exitCode)
		} else if statusName == "警告終了" {
			e.notify(node, "warning", sessionID, "警告終了", exitCode)
		} else {
			e.notify(node, "error", sessionID, "異常終了", exitCode)
		}
	}
	e.saveHistory()
	e.mu.Unlock()

	e.notifyStateChanged(sessionID)
}

func (e *Engine) replaceVariables(cmdStr string, session *JobSession, node *JobNode) string {
	// ジョブ変数の置換
	// ${START_DATE}, ${SESSION_ID}, ${TRIGGER_TYPE}, ${TRIGGER_INFO}
	cmdStr = strings.ReplaceAll(cmdStr, "${START_DATE}", session.StartDate)
	cmdStr = strings.ReplaceAll(cmdStr, "${SESSION_ID}", session.SessionID)
	cmdStr = strings.ReplaceAll(cmdStr, "${TRIGGER_TYPE}", session.TriggerType)
	cmdStr = strings.ReplaceAll(cmdStr, "${TRIGGER_INFO}", session.TriggerInfo)

	// 親ユニットのユーザ変数も置換
	if node.ParentID != "" {
		var findUnit func(id string) *JobNode
		findUnit = func(id string) *JobNode {
			n := e.jobs[id]
			if n == nil {
				return nil
			}
			if n.Type == TypeUnit {
				return n
			}
			return findUnit(n.ParentID)
		}
		unit := findUnit(node.ParentID)
		if unit != nil {
			// 簡易変数置換
		}
	}

	return cmdStr
}

func (e *Engine) StopNode(sessionID string, jobID string, control string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	session := e.findSession(sessionID)
	if session == nil {
		return
	}

	state := session.Nodes[jobID]
	if state == nil {
		return
	}

	if control == "保留" {
		state.Status = "保留中"
	} else if control == "保留解除" {
		if state.Status == "保留中" {
			state.Status = "待機中"
		}
	} else if control == "スキップ" {
		state.Status = "スキップ"
	} else if control == "中断" {
		// ジョブネットなどの実行中ノードを中断にする
		state.Status = "中断"
		state.EndDate = time.Now().Format("2006-01-02 15:04:05")
	} else if control == "コマンド" {
		// 実行中のコマンドを停止
		if state.Status == "実行中" {
			state.Status = "コマンド停止"
			state.EndDate = time.Now().Format("2006-01-02 15:04:05")
			cmdKey := sessionID + ":" + jobID
			if cmd, exists := e.activeCmds[cmdKey]; exists {
				// 停止コマンドがあればそれを実行
				node := e.jobs[jobID]
				if node != nil && node.StopCommand != "" {
					stopCmdStr := e.replaceVariables(node.StopCommand, session, node)
					var stopCmd *exec.Cmd
					if runtime.GOOS == "windows" {
						stopCmd = exec.Command("cmd.exe", "/c", stopCmdStr)
					} else {
						stopCmd = exec.Command("sh", "-c", stopCmdStr)
					}
					_ = stopCmd.Run()
				}
				// プロセス強制終了
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
			}
		}
	}

	e.saveHistory()
	go e.notifyStateChanged(sessionID)
}

// ヘルパーメソッド群
func (e *Engine) getSession(sessionID string) *JobSession {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.findSession(sessionID)
}

func (e *Engine) findSession(sessionID string) *JobSession {
	for _, s := range e.sessions {
		if s.SessionID == sessionID {
			return s
		}
	}
	return nil
}

func (e *Engine) getStateChan(sessionID string) chan bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stateChanged[sessionID]
}

func (e *Engine) notifyStateChanged(sessionID string) {
	e.mu.Lock()
	ch, exists := e.stateChanged[sessionID]
	e.mu.Unlock()
	if exists {
		select {
		case ch <- true:
		default:
		}
	}
}

func (e *Engine) cleanupSession(sessionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.stateChanged, sessionID)
}

func (e *Engine) StartMonitoring() {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for range ticker.C {
			e.checkAllMonitors()
		}
	}()
}

func (e *Engine) checkAllMonitors() {
	e.mu.Lock()
	var monitorsCopy []MonitorSetting
	for _, m := range e.monitors {
		monitorsCopy = append(monitorsCopy, *m)
	}
	e.mu.Unlock()

	for _, m := range monitorsCopy {
		e.runMonitor(&m)
	}
}

func (e *Engine) runMonitor(m *MonitorSetting) {
	e.mu.Lock()
	node, exists := e.nodes[m.NodeID]
	e.mu.Unlock()
	if !exists {
		return
	}

	status := "正常"
	logDetail := "Success"

	switch m.Type {
	case "ping":
		var cmd *exec.Cmd
		if runtime.GOOS == "windows" {
			cmd = exec.Command("ping", "-n", "1", "-w", "1000", node.IPAddress)
		} else {
			cmd = exec.Command("ping", "-c", "1", "-W", "1", node.IPAddress)
		}
		if err := cmd.Run(); err != nil {
			status = "異常"
			logDetail = fmt.Sprintf("Ping failed: %v", err)
		}
	case "http":
		client := http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get(m.Target)
		if err != nil {
			status = "異常"
			logDetail = fmt.Sprintf("HTTP request failed: %v", err)
		} else {
			resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 400 {
				status = "異常"
				logDetail = fmt.Sprintf("HTTP status code: %d", resp.StatusCode)
			}
		}
	case "port":
		addr := net.JoinHostPort(node.IPAddress, m.Target)
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			status = "異常"
			logDetail = fmt.Sprintf("TCP Connection failed to %s: %v", addr, err)
		} else {
			conn.Close()
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	orig, exists := e.monitors[m.ID]
	if !exists {
		return
	}

	statusChanged := orig.LastStatus != status && orig.LastStatus != "未実施"

	orig.LastStatus = status
	orig.LastCheck = time.Now().Format("2006-01-02 15:04:05")

	historyItem := MonitorHistory{
		CheckTime: orig.LastCheck,
		MonitorID: orig.ID,
		Name:      orig.Name,
		Type:      orig.Type,
		NodeName:  node.Name,
		Status:    status,
		Log:       logDetail,
	}
	e.monHistory = append([]MonitorHistory{historyItem}, e.monHistory...)
	if len(e.monHistory) > 50 {
		e.monHistory = e.monHistory[:50]
	}

	e.saveMonitors()
	e.saveMonHistory()

	if statusChanged {
		e.notifyMonitor(orig, node, status, logDetail)
	}
}

func (e *Engine) notifyMonitor(m *MonitorSetting, node *ManagedNode, status string, logDetail string) {
	notifyType := e.settings.DefaultNotify
	if notifyType == "" || notifyType == "none" {
		return
	}

	subject := fmt.Sprintf("[%s] Node Monitor: %s (Node: %s)", status, m.Name, node.Name)
	body := fmt.Sprintf("Monitor: %s\nType: %s\nNode: %s (IP: %s)\nStatus: %s\nLog: %s\nTime: %s\n",
		m.Name, m.Type, node.Name, node.IPAddress, status, logDetail, time.Now().Format("2006-01-02 15:04:05"))

	if notifyType == "slack" || notifyType == "both" {
		go sendSlack(e.settings.SlackWebhook, fmt.Sprintf("*%s*\n%s", subject, body))
	}
	if notifyType == "email" || notifyType == "both" {
		go sendEmail(e.settings, subject, body)
	}
}

func StartScheduler() {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for {
			<-ticker.C
			now := time.Now()
			engine.mu.Lock()
			schedules := make([]Schedule, len(engine.schedules))
			copy(schedules, engine.schedules)
			engine.mu.Unlock()

			for _, sched := range schedules {
				if !sched.Enabled {
					continue
				}
				if matchSchedule(sched, now) {
					fmt.Printf("[Scheduler] Triggering job: %s (Schedule: %s)\n", sched.JobID, sched.Name)
					sessionID := engine.StartSession(sched.JobID, "スケジュール", sched.Name)
					if sessionID == "" {
						fmt.Printf("[Scheduler Error] Failed to start session: Job '%s' does not exist (Schedule: %s)\n", sched.JobID, sched.Name)
					}
				}
			}
		}
	}()
}

func matchSchedule(sched Schedule, t time.Time) bool {
	if !sched.Enabled {
		return false
	}
	hour, _ := strconv.Atoi(sched.Hour)
	minute, _ := strconv.Atoi(sched.Minute)
	interval := sched.Interval
	if interval <= 0 {
		interval = 1
	}

		switch sched.Type {
	case "cron":
		return matchCronSchedule(sched.CronExpr, t)
	case "daily":
		return t.Hour() == hour && t.Minute() == minute
	case "weekly":
		wdayStr := fmt.Sprintf("%d", t.Weekday())
		if sched.Weekday != "*" && sched.Weekday != wdayStr {
			return false
		}
		return t.Hour() == hour && t.Minute() == minute
	case "hourly":
		startMin := minute
		if t.Minute() < startMin {
			return false
		}
		return (t.Minute()-startMin)%interval == 0
	case "interval":
		// 本日の開始時刻からの経過分で判定
		startMinOfDay := hour*60 + minute
		currentMinOfDay := t.Hour()*60 + t.Minute()
		if currentMinOfDay < startMinOfDay {
			return false
		}
		return (currentMinOfDay-startMinOfDay)%interval == 0
	default:
		// 日時指定 (datetime)
		mStr := fmt.Sprintf("%d", t.Month())
		dStr := fmt.Sprintf("%d", t.Day())
		if sched.Month != "*" && sched.Month != mStr {
			return false
		}
		if sched.Day != "*" && sched.Day != dStr {
			return false
		}
		return t.Hour() == hour && t.Minute() == minute
	}
}

// --- Web UI & Handler ---

const htmlTemplate = `<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>Himenos クライアント</title>
    <style>
        /* 全体スタイル - Windowsクラシック / Eclipse RCP風 */
        body {
            font-family: 'MS PGothic', 'Meiryo', sans-serif;
            background: #f0f0f0;
            color: #000;
            margin: 0;
            padding: 0;
            font-size: 12px;
        }

        /* ツールバー (タブ・パースペクティブ) */
        .toolbar {
            background: #f5f5f5;
            border-bottom: 1px solid #b0b0b0;
            padding: 6px 10px;
            display: flex;
            gap: 5px;
            align-items: center;
        }
        .tool-btn {
            background: transparent;
            border: 1px solid transparent;
            color: #333;
            padding: 4px 10px;
            font-size: 12px;
            cursor: pointer;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 3px;
        }
        .tool-btn:hover {
            background: #e5e5e5;
            border-color: #ccc;
        }
        .tool-active {
            background: #ffffff;
            border: 1px solid #b0b0b0;
            border-bottom-color: #ffffff;
            font-weight: bold;
            color: #000;
            margin-bottom: -7px;
            padding-bottom: 6px;
            border-top: 3px solid #1d50a2;
        }

        /* コンテナ */
        .container {
            padding: 5px;
            display: flex;
            gap: 5px;
            box-sizing: border-box;
            height: calc(100vh - 69px);
        }
        .pane-left {
            width: 30%;
            background: #ffffff;
            border: 1px solid #b0b0b0;
            display: flex;
            flex-direction: column;
            box-sizing: border-box;
        }
        .pane-right {
            width: 70%;
            background: #ffffff;
            border: 1px solid #b0b0b0;
            display: flex;
            flex-direction: column;
            box-sizing: border-box;
        }
        .pane-full {
            width: 100%;
            background: #ffffff;
            border: 1px solid #b0b0b0;
            padding: 10px;
            box-sizing: border-box;
            overflow-y: auto;
        }

        /* ビューのタイトルバー */
        .view-title {
            background: linear-gradient(to bottom, #e1e9f6, #c5d7ed);
            border-bottom: 1px solid #99b4d1;
            padding: 4px 8px;
            font-weight: bold;
            color: #1e395b;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .view-content {
            padding: 8px;
            flex: 1;
            overflow-y: auto;
            box-sizing: border-box;
        }

        /* ツリーノード */
        .tree-node {
            padding: 2px;
            white-space: pre-wrap;
        }
        .tree-active {
            background: #3399ff;
            color: #ffffff;
        }
        .tree-active a {
            color: #ffffff;
        }
        .tree-link {
            text-decoration: none;
            color: #000;
        }
        .tree-link:hover {
            text-decoration: underline;
        }

        /* テーブル */
        table {
            width: 100%;
            border-collapse: collapse;
            border: 1px solid #ccc;
            margin-bottom: 10px;
        }
        th, td {
            border: 1px solid #ccc;
            padding: 4px 6px;
            font-size: 12px;
            text-align: left;
        }
        th {
            background: linear-gradient(to bottom, #ffffff, #ebebeb);
            color: #000;
            font-weight: normal;
            border-bottom: 2px solid #ccc;
        }
        tr:hover {
            background: #f2f7fc;
        }

        /* ボタン */
        button, .btn {
            background: linear-gradient(to bottom, #fcfcfc, #e6e6e6);
            border: 1px solid #b0b0b0;
            color: #000;
            padding: 3px 10px;
            font-size: 12px;
            border-radius: 2px;
            cursor: pointer;
            text-decoration: none;
            display: inline-block;
        }
        button:hover, .btn:hover {
            background: linear-gradient(to bottom, #f2f2f2, #dcdcdc);
            border-color: #999;
        }
        .btn-danger {
            background: linear-gradient(to bottom, #fff5f5, #ffe0e0);
            border-color: #cc9999;
            color: #cc0000;
        }
        .btn-danger:hover {
            background: #ffcccc;
        }
        .btn-primary {
            background: linear-gradient(to bottom, #e3f2fd, #bbdefb);
            border-color: #90caf9;
            color: #0d47a1;
            font-weight: bold;
        }
        .btn-primary:hover {
            background: #90caf9;
        }

        /* 下部ステータス集計バー (Himenos再現) */
        .summary-bar {
            border-top: 1px solid #b0b0b0;
            background: #e1e1e1;
            display: flex;
            font-size: 11px;
            align-items: stretch;
            height: 22px;
        }
        .summary-item {
            display: flex;
            align-items: center;
            justify-content: center;
            width: 100px;
            color: #000;
            font-weight: bold;
            border-right: 1px solid #b0b0b0;
            text-decoration: none;
        }
        .summary-item:hover {
            opacity: 0.8;
        }
        .summary-red { background: #ff4d4d; color: #fff; }
        .summary-yellow { background: #ffeb3b; color: #000; }
        .summary-green { background: #4caf50; color: #fff; }
        .summary-blue { background: #2196f3; color: #fff; }
        .summary-count {
            margin-left: auto;
            padding: 0 10px;
            display: flex;
            align-items: center;
            color: #333;
            font-weight: bold;
        }

        /* 最下部システムステータスバー */
        .status-bar {
            background: #e1e1e1;
            border-top: 1px solid #b0b0b0;
            padding: 3px 10px;
            font-size: 11px;
            color: #333;
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            height: 18px;
            display: flex;
            justify-content: space-between;
        }

        /* ステータス表示バッジ */
        .badge { display: inline-block; padding: 1px 4px; border-radius: 2px; font-size: 11px; font-weight: bold; }
        .status-running { background: #2196f3; color: #fff; }
        .status-success { background: #4caf50; color: #fff; }
        .status-warn { background: #ffeb3b; color: #000; }
        .status-error { background: #ff4d4d; color: #fff; }
        .status-hold { background: #9e9e9e; color: #fff; }

        label { display: block; font-weight: bold; margin-top: 10px; margin-bottom: 5px; }
        .help-text { color: #666; font-size: 11px; margin-bottom: 5px; }
        .error-message { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; padding: 8px; margin-bottom: 10px; border-radius: 4px; }
        
        .refresh-icon { text-decoration: none; color: #1e395b; font-weight: bold; font-size: 12px; }
        .refresh-icon:hover { color: #1d50a2; }

        /* ジョブフロー進捗マップ用スタイル */
        .flow-container {
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            margin-top: 15px;
            margin-bottom: 15px;
            padding: 10px;
            background: #fafafa;
            border: 1px dashed #ccc;
            align-items: center;
        }
        .flow-node {
            padding: 6px 12px;
            border: 1px solid #999;
            border-radius: 4px;
            font-weight: bold;
            min-width: 100px;
            text-align: center;
            box-shadow: 1px 1px 3px rgba(0,0,0,0.1);
        }
        .flow-node-wait { background: #d3d3d3; color: #333; }
        .flow-node-run { background: #2196f3; color: #fff; }
        .flow-node-success { background: #4caf50; color: #fff; }
        .flow-node-warn { background: #ffeb3b; color: #000; }
        .flow-node-error { background: #ff4d4d; color: #fff; }
        .flow-arrow { font-size: 16px; font-weight: bold; color: #666; }

        /* トポロジー監視マップ用スタイル */
        .topology-map {
            background: #ffffff;
            border: 1px solid #ccc;
            padding: 20px;
            font-family: monospace;
            white-space: pre;
            overflow-x: auto;
            line-height: 1.5;
        }
        .topo-node {
            display: inline-block;
            border: 1px solid #333;
            background: #f0f0f0;
            padding: 4px 8px;
            border-radius: 3px;
            font-weight: bold;
            font-family: sans-serif;
        }
        .topo-node a {
            text-decoration: none;
            color: #000;
        }
        .topo-ok { background: #4caf50; color: #fff; }
        .topo-err { background: #ff4d4d; color: #fff; }
        .topo-warn { background: #ffeb3b; color: #000; }
        .topo-unknown { background: #2196f3; color: #fff; }

        /* 3分割画面レイアウト用 */
        .split-layout {
            display: flex;
            flex-direction: column;
            gap: 5px;
            height: 100%;
            overflow-y: auto;
        }
        .split-section {
            background: #ffffff;
            border: 1px solid #b0b0b0;
            display: flex;
            flex-direction: column;
            box-sizing: border-box;
            min-height: 150px;
        }
    </style>
    <script>
        function updateScheduleForm() {
            const checkedRadio = document.querySelector('input[name="type"]:checked');
            if (!checkedRadio) return;
            const type = checkedRadio.value;
            
            const runDate = document.getElementById('input_run_date');
            const cronExpr = document.getElementById('input_cron_expr');
            const weekday = document.getElementById('input_weekday');
            const hour = document.getElementById('input_hour');
            const minute = document.getElementById('input_minute');
            const interval = document.getElementById('input_interval');

            if (!runDate) return; // スケジュール設定画面以外のタブでは何もしない

            // 一旦すべての項目を無効化 (disabled)
            runDate.disabled = true;
            cronExpr.disabled = true;
            weekday.disabled = true;
            hour.disabled = true;
            minute.disabled = true;
            interval.disabled = true;

            // スタイルをグレーアウト化
            [runDate, cronExpr, weekday, hour, minute, interval].forEach(el => {
                el.style.background = '#e1e1e1';
                el.style.color = '#888';
                el.style.cursor = 'not-allowed';
            });

            // 選択した種別に対応する項目を有効化 (enabled)
            if (type === 'daily') {
                hour.disabled = false;
                minute.disabled = false;
            } else if (type === 'weekly') {
                weekday.disabled = false;
                hour.disabled = false;
                minute.disabled = false;
            } else if (type === 'hourly') {
                minute.disabled = false;
                interval.disabled = false;
            } else if (type === 'interval') {
                hour.disabled = false;
                minute.disabled = false;
                interval.disabled = false;
            } else if (type === 'datetime') {
                runDate.disabled = false;
                hour.disabled = false;
                minute.disabled = false;
            } else if (type === 'cron') {
                cronExpr.disabled = false;
            }

            // 有効化された項目のスタイルを復元
            [runDate, cronExpr, weekday, hour, minute, interval].forEach(el => {
                if (!el.disabled) {
                    el.style.background = '#ffffff';
                    el.style.color = '#000';
                    el.style.cursor = 'auto';
                }
            });
        }

        function toggleNewScriptFields() {
            const select = document.getElementById('script_select');
            const fields = document.getElementById('new_script_fields');
            if (select && fields) {
                if (select.value === '__NEW__') {
                    fields.style.display = 'block';
                } else {
                    fields.style.display = 'none';
                }
            }
        }

        // DOM構築完了時、および全体読み込み完了時に実行
        window.addEventListener('DOMContentLoaded', updateScheduleForm);
        window.addEventListener('load', updateScheduleForm);
        window.addEventListener('DOMContentLoaded', toggleNewScriptFields);
    </script>
</head>
<body>

<div class="toolbar">
    <a href="/?tab=jobs" class="tool-btn {{if or (eq .CurrentTab "jobs") (eq .CurrentTab "history") (eq .CurrentTab "schedules") (eq .CurrentTab "script_edit") (eq .CurrentTab "job_new") (eq .CurrentTab "job_edit")}}tool-active{{end}}">📋 ジョブ管理</a>
    <a href="/?tab=nodes" class="tool-btn {{if or (eq .CurrentTab "nodes") (eq .CurrentTab "topology") (eq .CurrentTab "monitors") (eq .CurrentTab "nodes_manage") (eq .CurrentTab "node_new")}}tool-active{{end}}">🖥️ ノード・監視</a>
    <a href="/?tab=settings" class="tool-btn {{if eq .CurrentTab "settings"}}tool-active{{end}}">⚙️ 環境構築</a>
</div>

<div class="container">
    {{if or (eq .CurrentTab "jobs") (eq .CurrentTab "history") (eq .CurrentTab "schedules") (eq .CurrentTab "script_edit") (eq .CurrentTab "job_new") (eq .CurrentTab "job_edit") (eq .CurrentTab "jobs_manage")}}
        <!-- ジョブ管理 統合画面 -->
        <div class="split-layout">
            <!-- 1. ジョブ設定セクション -->
            <div class="split-section" style="min-height: 400px; display: flex; flex-direction: row; gap: 5px; height: 450px;">
                 <!-- 左ペイン：ジョブツリー -->
                 <div class="pane-left" style="flex: 1; border: none; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title">
                         <span>ジョブ定義[一覧]</span>
                         <a href="/?tab=jobs{{if .SelectedJobID}}&s={{.SelectedJobID}}{{end}}" class="refresh-icon">🔄 更新</a>
                     </div>
                     <div class="view-content">
                         <div style="margin-bottom: 10px; display: flex; gap: 5px;">
                             <a href="/?tab=job_new&type=job" class="btn btn-primary" style="flex:1; text-align:center;">＋ ジョブ作成</a>
                             <a href="/?tab=job_new&type=unit" class="btn" style="flex:1; text-align:center;">＋ ユニット作成</a>
                         </div>
                         {{range .JobTree}}
                             <div class="tree-node {{if eq .Node.ID $.SelectedJobID}}tree-active{{end}}">
                                 {{.Prefix}}{{if eq .Node.Type "unit"}}🏢{{else if eq .Node.Type "net"}}📂{{else}}📄{{end}} <a href="/?tab=jobs&s={{.Node.ID}}" class="tree-link">{{.Node.Name}}</a>
                             </div>
                         {{else}}
                             <div>ジョブが登録されていません。</div>
                         {{end}}

                         <h4 style="margin-top:20px; border-bottom:1px solid #ccc; padding-bottom:3px;">📁 実スクリプトファイル一覧</h4>
                         <div class="tree-node">
                             📁 scripts
                         </div>
                         {{range .ScriptFiles}}
                             <div class="tree-node">
                                 {{.Prefix}}📝 <a href="/?tab=script_edit&file={{.Path}}" class="tree-link">
                                     {{if .IsEnabled}}
                                         {{.Name}}
                                     {{else}}
                                         <span style="text-decoration: line-through; color: #888;">{{.Name}} (無効化中)</span>
                                     {{end}}
                                 </a>
                             </div>
                         {{else}}
                             <div class="tree-node">   └─ scripts/ フォルダにファイルがありません。</div>
                         {{end}}
                     </div>
                 </div>

                 <!-- 右ペイン：詳細 / 新規 / 編集 / スクリプト編集 -->
                 <div class="pane-right" style="flex: 2; border: none; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     {{if eq .CurrentTab "job_new"}}
                          <div class="view-title"><span>新規作成 ({{if eq .NewNodeType "unit"}}ユニット{{else if eq .NewNodeType "net"}}ネット{{else}}ジョブ{{end}})</span></div>
                          <div class="view-content">
                              {{if .ErrorMessage}}
                                  <div class="error-message">{{.ErrorMessage}}</div>
                              {{end}}
                              <form action="/action" method="POST">
                                  <input type="hidden" name="action" value="create_job">
                                  <input type="hidden" name="type" value="{{.NewNodeType}}">
                                  <input type="hidden" name="parent_id" value="{{.ParentID}}">

                                  <label>名前:</label>
                                  <input type="text" name="name" required placeholder="例: バックアップ処理">

                                  {{if eq .NewNodeType "job"}}
                                      <label>実行スクリプトファイル:</label>
                                      <select name="script_select" id="script_select" onchange="toggleNewScriptFields()">
                                          <option value="__NEW__">＋ 新規スクリプトファイルを作成する</option>
                                          {{range .ScriptFiles}}
                                              {{if .IsEnabled}}
                                                  <option value="{{.Path}}" {{if eq .Path $.SelectedJobID}}selected{{end}}>{{.Name}} (既存ファイル)</option>
                                              {{end}}
                                          {{end}}
                                      </select>

                                      <div id="new_script_fields" style="display: none;">
                                          <label>新規作成ファイル名 (新規作成時のみ):</label>
                                          <input type="text" name="script_filename" placeholder="例: backup.sh">

                                          <label>スクリプト内容 (新規作成時のみ):</label>
                                          <textarea name="script_content" rows="10" placeholder="#!/bin/sh&#10;echo &quot;Processing...&quot;&#10;exit 0"></textarea>
                                      </div>

                                      <label>引数 (任意):</label>
                                      <input type="text" name="run_user" placeholder="例: 10 daily">
                                  {{end}}

                                  {{if ne .NewNodeType "unit"}}
                                      <label>待ち条件 (先行ジョブ、複数ある場合はカンマ区切り):</label>
                                      <div class="help-text">指定したジョブが正常終了すると起動します。ジョブ名で指定してください。</div>
                                      <input type="text" name="wait_conditions" placeholder="例: 前処理ジョブ, DB停止ジョブ">
                                  {{end}}

                                  <label>個別の通知設定:</label>
                                  <select name="notify_all">
                                      <option value="default">デフォルト設定に従う</option>
                                      <option value="none">例外: 通知しない</option>
                                      <option value="both">例外: メール & Slack 両方</option>
                                      <option value="slack">例外: Slack のみ</option>
                                      <option value="email">例外: メール のみ</option>
                                  </select>

                                  <div style="margin-top: 15px;">
                                      <button type="submit">💾 作成して保存</button>
                                      <a href="/?tab=jobs" class="btn">キャンセル</a>
                                  </div>
                              </form>
                          </div>
                     {{else if eq .CurrentTab "job_edit"}}
                          <div class="view-title"><span>ジョブ定義編集: {{.SelectedNode.Name}}</span></div>
                          <div class="view-content">
                              {{if .ErrorMessage}}
                                  <div class="error-message">{{.ErrorMessage}}</div>
                              {{end}}
                              <form action="/action" method="POST">
                                  <input type="hidden" name="action" value="update_job">
                                  <input type="hidden" name="id" value="{{.SelectedNode.ID}}">

                                  <label>名前:</label>
                                  <input type="text" name="name" value="{{.SelectedNode.Name}}" required>

                                  {{if eq .SelectedNode.Type "job"}}
                                      <label>実行スクリプトパス:</label>
                                      <input type="text" name="command" value="{{.SelectedNode.Command}}" style="width: 50%;">
                                      <span style="font-size:11px; color:#666;">（実ファイルを編集したい場合は、直接編集タブや左側メニューからファイルを選択してください）</span>

                                      <label>引数 (任意):</label>
                                      <input type="text" name="run_user" value="{{.SelectedNode.RunUser}}">
                                  {{end}}

                                  {{if ne .SelectedNode.Type "unit"}}
                                      <label>待ち条件 (先行ジョブ、複数ある場合はカンマ区切り):</label>
                                      <input type="text" name="wait_conditions" value="{{.WaitConditionsStr}}" placeholder="例: 前処理ジョブ, DB停止ジョブ">
                                  {{end}}

                                  <label>個別の通知設定:</label>
                                  <select name="notify_all">
                                      <option value="default" {{if eq .SelectedNode.NotifyNormal "default"}}selected{{end}}>デフォルト設定に従う</option>
                                      <option value="none" {{if eq .SelectedNode.NotifyNormal "none"}}selected{{end}}>例外: 通知しない</option>
                                      <option value="both" {{if eq .SelectedNode.NotifyNormal "both"}}selected{{end}}>例外: メール & Slack 両方</option>
                                      <option value="slack" {{if eq .SelectedNode.NotifyNormal "slack"}}selected{{end}}>例外: Slack のみ</option>
                                      <option value="email" {{if eq .SelectedNode.NotifyNormal "email"}}selected{{end}}>例外: メール のみ</option>
                                  </select>

                                  <div style="margin-top: 15px;">
                                      <button type="submit">💾 変更を保存</button>
                                      <a href="/?tab=jobs&s={{.SelectedNode.ID}}" class="btn">キャンセル</a>
                                  </div>
                              </form>
                          </div>
                     {{else if eq .CurrentTab "script_edit"}}
                          <div class="view-title"><span>スクリプトファイルの直接編集: {{.SelectedJobID}}</span></div>
                          <div class="view-content">
                              {{if .ErrorMessage}}
                                  <div class="error-message">{{.ErrorMessage}}</div>
                              {{end}}
                              <form action="/action" method="POST">
                                  <input type="hidden" name="action" value="save_script_only">
                                  <input type="hidden" name="filepath" value="{{.SelectedJobID}}">

                                  <label>スクリプトパス:</label>
                                  <input type="text" value="{{.SelectedJobID}}" readonly style="background:#e0e0e0; cursor:not-allowed; width:50%;">

                                  <label>スクリプト本文 (Git連携):</label>
                                  <textarea name="script_content" rows="15" style="width:100%;">{{.ScriptContent}}</textarea>

                                  <div style="margin-top:10px;">
                                      <button type="submit">💾 ファイル保存</button>
                                      <a href="/?tab=jobs" class="btn">キャンセル</a>
                                  </div>
                              </form>

                              <form action="/action" method="POST" style="margin-top: 15px; border-top: 1px dashed #ccc; padding-top: 15px;">
                                  <input type="hidden" name="action" value="toggle_script">
                                  <input type="hidden" name="file" value="{{.SelectedJobID}}">
                                  {{if .SelectedScriptEnabled}}
                                      <button type="submit" class="btn btn-danger">🚫 実スクリプトを無効化する</button>
                                  {{else}}
                                      <button type="submit" class="btn btn-success" style="background-color: #28a745; color: white;">✅ 実スクリプトを有効化する</button>
                                  {{end}}
                              </form>

                              <div style="margin-top:20px; padding:10px; background:#f9f9f9; border:1px solid #ccc;">
                                  <h4>このスクリプトにバインドされているジョブ</h4>
                                  {{if .SelectedNode}}
                                      <p>ジョブ: <strong>{{.SelectedNode.Name}}</strong> で設定されています。</p>
                                      <div style="display: flex; gap: 5px; align-items: center;">
                                          <form action="/action" method="POST" style="margin:0;">
                                              <input type="hidden" name="action" value="run_job">
                                              <input type="hidden" name="id" value="{{.SelectedNode.ID}}">
                                              <button type="submit">⚡ ジョブの実行</button>
                                          </form>
                                          <a href="/?tab=jobs&s={{.SelectedNode.ID}}" class="btn">ジョブ定義を表示</a>
                                      </div>
                                  {{else}}
                                      <p style="color:#666;">このスクリプトを参照している有効なジョブは現在ありません。</p>
                                  {{end}}
                              </div>
                          </div>
                     {{else}}
                          <div class="view-title"><span>ジョブ定義[詳細]</span></div>
                          <div class="view-content">
                              {{if .SelectedNode}}
                                  <h3>{{.SelectedNode.Name}}</h3>
                                  <table border="1" cellspacing="0" cellpadding="4">
                                      <tr><th style="width: 30%;">項目</th><th>設定値</th></tr>
                                      <tr><th>ジョブ名</th><td>{{.SelectedNode.Name}}</td></tr>
                                      {{if eq .SelectedNode.Type "job"}}
                                          <tr><th>実行スクリプト</th><td><code>{{.SelectedNode.Command}}</code></td></tr>
                                          {{if .SelectedNode.RunUser}}
                                              <tr><th>引数</th><td><code>{{.SelectedNode.RunUser}}</code></td></tr>
                                          {{end}}
                                      {{end}}
                                      {{if .SelectedNode.WaitConditions}}
                                          <tr><th>待ち条件 (先行ジョブ)</th><td>
                                              {{range .SelectedNode.WaitConditions}}
                                                  {{$targetNode := index $.JobMap .JobID}}
                                                  {{if $targetNode}}{{$targetNode.Name}}{{else}}{{.JobID}}{{end}} (正常終了待ち)<br>
                                              {{end}}
                                          </td></tr>
                                      {{end}}
                                      <tr><th>通知設定</th><td>
                                          {{if eq .SelectedNode.NotifyNormal "default"}}デフォルト設定に従う{{else if eq .SelectedNode.NotifyNormal "none"}}例外: 通知しない{{else if eq .SelectedNode.NotifyNormal "both"}}例外: メール・Slack通知{{else if eq .SelectedNode.NotifyNormal "slack"}}例外: Slack通知{{else if eq .SelectedNode.NotifyNormal "email"}}例外: メール通知{{else}}デフォルト設定に従う{{end}}
                                      </td></tr>
                                  </table>

                                  <div style="margin-top: 15px; display: flex; gap: 5px;">
                                      {{if or (eq .SelectedNode.Type "unit") (and (eq .SelectedNode.Type "job") .SelectedNode.Command)}}
                                          <form action="/action" method="POST" style="margin:0;">
                                              <input type="hidden" name="action" value="run_job">
                                              <input type="hidden" name="id" value="{{.SelectedNode.ID}}">
                                              <button type="submit">⚡ ジョブの実行</button>
                                          </form>
                                      {{end}}
                                      <a href="/?tab=job_edit&id={{.SelectedNode.ID}}" class="btn">✏️ 変更 / スクリプト編集</a>
                                      {{if or (eq .SelectedNode.Type "unit") (eq .SelectedNode.Type "net")}}
                                          <a href="/?tab=job_new&type=net&parent={{.SelectedNode.ID}}" class="btn">📂 下位ネット作成</a>
                                          <a href="/?tab=job_new&type=job&parent={{.SelectedNode.ID}}" class="btn btn-primary">📄 下位ジョブ作成</a>
                                      {{end}}
                                      <form action="/action" method="POST" style="margin:0;">
                                          <input type="hidden" name="action" value="delete_job">
                                          <input type="hidden" name="id" value="{{.SelectedNode.ID}}">
                                          <button type="submit" class="btn btn-danger" onclick="return confirm('本当に削除しますか？');">🗑️ 削除</button>
                                      </form>
                                  </div>
                              {{else}}
                                  <p>左側のツリーからジョブを選択してください。実スクリプトファイル名をクリックすると、スクリプトの直接編集が可能です。</p>
                              {{end}}
                          </div>
                     {{end}}
                 </div>
            </div>

            <!-- 2. ジョブ履歴セクション -->
            <div class="split-section" style="min-height: 250px; display: flex; flex-direction: row; gap: 5px; height: 300px;">
                 <div style="flex: 3; overflow-y: auto; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;">
                         <span>📊 ジョブ履歴</span>
                         <a href="/?tab=jobs" class="refresh-icon">🔄 更新</a>
                     </div>
                     <form action="/" method="GET" style="display: flex; gap: 5px; flex-wrap: wrap; margin-bottom: 10px; font-size:12px;">
                         <input type="hidden" name="tab" value="{{$.CurrentTab}}">
                         {{if $.SelectedJobID}}<input type="hidden" name="s" value="{{$.SelectedJobID}}">{{end}}
                         <input type="text" name="keyword" placeholder="キーワード検索" value="{{.HistoryKeyword}}" style="padding:2px; font-size:12px; width:120px;">
                         <input type="date" name="start_date" value="{{.SelectedJobID}}" style="padding:2px; font-size:12px;">
                         <span>～</span>
                         <input type="date" name="end_date" value="{{.WaitConditionsStr}}" style="padding:2px; font-size:12px;">
                         <button type="submit" style="padding:2px 8px; font-size:12px;">検索</button>
                         <a href="/?tab=jobs" style="padding:2px; font-size:12px;">クリア</a>
                     </form>

                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px;">
                         <thead>
                             <tr>
                                 <th>セッションID</th>
                                 <th>ユニット名</th>
                                 <th>開始日時</th>
                                 <th>状態</th>
                             </tr>
                         </thead>
                         <tbody>
                             {{range .Sessions}}
                                 <tr class="{{if eq .SessionID $.SelectedSessionID}}tree-active{{end}}">
                                     <td><a href="/?tab={{$.CurrentTab}}&session_id={{.SessionID}}{{if $.SelectedJobID}}&s={{$.SelectedJobID}}{{end}}">{{.SessionID}}</a></td>
                                     <td>{{.UnitName}}</td>
                                     <td>{{.StartDate}}</td>
                                     <td>
                                         {{if eq .Status "正常終了"}}<span class="badge status-success">正常終了</span>
                                         {{else if eq .Status "異常終了"}}<span class="badge status-danger">異常終了</span>
                                         {{else if eq .Status "警告終了"}}<span class="badge status-hold">警告終了</span>
                                         {{else}}<span class="badge status-run">実行中</span>{{end}}
                                     </td>
                                 </tr>
                             {{else}}
                                 <tr><td colspan="4">履歴がありません。</td></tr>
                             {{end}}
                         </tbody>
                     </table>
                 </div>

                 <div style="flex: 2; overflow-y: auto; padding: 10px; height: 100%; box-sizing: border-box;">
                     {{if .SelectedSession}}
                         <div class="view-title" style="margin: -10px -10px 10px -10px;"><span>セッション詳細: {{.SelectedSessionID}}</span></div>
                         <div style="font-size:12px; margin-bottom:10px;">
                             <strong>全体状態:</strong> 
                             {{if eq .SelectedSession.Status "正常終了"}}<span class="badge status-success">正常終了</span>
                             {{else if eq .SelectedSession.Status "異常終了"}}<span class="badge status-danger">異常終了</span>
                             {{else if eq .SelectedSession.Status "警告終了"}}<span class="badge status-hold">警告終了</span>
                             {{else}}<span class="badge status-run">実行中</span>{{end}}
                             (正常: {{.GreenCount}} / 警告: {{.YellowCount}} / 異常: {{.RedCount}} / 合計: {{.TotalCount}})
                         </div>

                         <div class="topology-map" style="padding:10px; font-size:11px; margin-bottom:10px; max-height:120px; overflow-y:auto;">
                             {{range .SessionNodes}}
                                 <span class="topo-node {{if eq .Node.Status "実行中"}}flow-node-run{{else if eq .Node.Status "未実施"}}flow-node-unknown{{else if eq .Node.Status "起動失敗"}}flow-node-error{{else if eq .StatusLabel "正常終了"}}flow-node-success{{else if eq .StatusLabel "警告終了"}}flow-node-warn{{else if eq .StatusLabel "異常終了"}}flow-node-error{{else}}flow-node-success{{end}}">
                                     <a href="/?tab={{$.CurrentTab}}&session_id={{$.SelectedSessionID}}&show_log={{.Node.JobID}}{{if $.SelectedJobID}}&s={{$.SelectedJobID}}{{end}}" style="color:inherit;">{{.Node.Name}} [{{if eq .Node.Status "終了"}}{{.StatusLabel}}{{else}}{{.Node.Status}}{{end}}]</a>
                                 </span>
                                 <span class="flow-arrow">→</span>
                             {{end}}
                             <span>完了</span>
                         </div>

                         {{if .ShowLogNode}}
                             <h5 style="margin:5px 0;">ログ: {{.ShowLogNode.Name}} (終了コード: {{.ShowLogNode.ExitValue}})</h5>
                             <textarea readonly rows="6" style="width:100%; font-family:monospace; font-size:11px; background:#fafafa;">{{.ShowLogNode.LogOutput}}</textarea>
                         {{else}}
                             <p style="color:#666; font-size:11px;">フロー内のジョブ名をクリックすると実行ログが表示されます。</p>
                         {{end}}
                     {{else}}
                         <p style="color:#666; font-size:12px;">履歴一覧からセッションを選択すると詳細が表示されます。</p>
                     {{end}}
                 </div>
            </div>

            <!-- 3. スケジュール設定セクション -->
            <div class="split-section" style="min-height: 300px; display: flex; flex-direction: row; gap: 5px; height: 350px; overflow-y: auto;">
                 <div style="flex: 1.5; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;">
                         <span>📅 スケジュール一覧</span>
                         <a href="/?tab=jobs" class="refresh-icon">🔄 更新</a>
                     </div>
                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px;">
                         <thead>
                             <tr>
                                 <th>スケジュール名</th>
                                 <th>実行対象ユニット/ジョブ</th>
                                 <th>設定</th>
                                 <th>状態</th>
                                 <th>操作</th>
                             </tr>
                         </thead>
                         <tbody>
                             {{range .Schedules}}
                                 <tr>
                                     <td>{{.Name}}</td>
                                     <td>
                                         {{$targetUnit := index $.JobMap .JobID}}
                                         {{if $targetUnit}}
                                             {{$targetUnit.Name}}
                                         {{else}}
                                             <span style="color: red; font-weight: bold;">⚠️ 存在しないジョブ: {{.JobID}}</span>
                                         {{end}}
                                     </td>
                                     <td>
                                         {{if eq .Type "weekly"}}毎週: {{if eq .Weekday "0"}}日曜日{{else if eq .Weekday "1"}}月曜日{{else if eq .Weekday "2"}}火曜日{{else if eq .Weekday "3"}}水曜日{{else if eq .Weekday "4"}}木曜日{{else if eq .Weekday "5"}}金曜日{{else if eq .Weekday "6"}}土曜日{{else}}毎日{{end}} @ {{.Hour}}:{{.Minute}}
                                         {{else if eq .Type "daily"}}毎日 @ {{.Hour}}:{{.Minute}}
                                         {{else if eq .Type "hourly"}}毎時: {{.Minute}}分から {{.Interval}}分間隔
                                         {{else if eq .Type "interval"}}一定間隔: {{.Hour}}:{{.Minute}}から {{.Interval}}分間隔
                                         {{else if eq .Type "cron"}}crontab: <code>{{.CronExpr}}</code>
                                         {{else}}日時指定: {{.Month}}/{{.Day}} @ {{.Hour}}:{{.Minute}}{{end}}
                                     </td>
                                     <td>
                                         {{if .Enabled}}<span class="badge status-success">有効</span>{{else}}<span class="badge status-hold">無効</span>{{end}}
                                     </td>
                                     <td>
                                         <form action="/action" method="POST" style="display:inline;">
                                             <input type="hidden" name="action" value="toggle_schedule">
                                             <input type="hidden" name="id" value="{{.ID}}">
                                             <button type="submit" class="btn">{{if .Enabled}}無効化{{else}}有効化{{end}}</button>
                                         </form>
                                         <form action="/action" method="POST" style="display:inline;">
                                             <input type="hidden" name="action" value="delete_schedule">
                                             <input type="hidden" name="id" value="{{.ID}}">
                                             <button type="submit" class="btn btn-danger">削除</button>
                                         </form>
                                     </td>
                                 </tr>
                             {{else}}
                                 <tr><td colspan="5">スケジュールが設定されていません。</td></tr>
                             {{end}}
                         </tbody>
                     </table>

                     <h4 style="margin-top:20px;">📝 スケジュール一括登録・編集 (crontab -e 互換)</h4>
                     <form action="/action" method="POST">
                         <input type="hidden" name="action" value="save_schedules_bulk">
                         <textarea name="schedules_cron" rows="6" style="width:100%; font-family:monospace; font-size:12px;">{{.SchedulesCronText}}</textarea>
                         <div style="margin-top:5px;">
                             <button type="submit" class="btn btn-primary" style="font-size:12px; padding:3px 10px;">💾 スケジュール一括保存 (適用)</button>
                         </div>
                     </form>
                 </div>

                 <div style="flex: 1; padding: 10px; height: 100%; box-sizing: border-box; overflow-y: auto; font-size:12px;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;"><span>➕ スケジュール追加</span></div>
                     <form action="/action" method="POST">
                         <input type="hidden" name="action" value="create_schedule">
                         
                         <label style="margin-top:3px;">スケジュール名:</label>
                         <input type="text" name="name" required placeholder="例: 夜間バックアップ" style="width:100%; font-size:12px; padding:2px;">

                         <label style="margin-top:5px;">実行対象ジョブ:</label>
                         <select name="job_id" style="width:100%; font-size:12px; padding:2px;">
                             {{range .UnitOptions}}
                                 <option value="{{.ID}}">{{.Name}}</option>
                             {{end}}
                         </select>

                         <label style="margin-top:5px;">設定タイプ:</label>
                         <div style="display: flex; flex-direction: column; gap: 2px; margin-bottom: 5px;">
                             <label style="font-weight:normal; margin-top:2px; font-size:12px;"><input type="radio" name="type" value="cron" checked onclick="updateScheduleForm()"> crontab形式</label>
                             <label style="font-weight:normal; margin-top:2px; font-size:12px;"><input type="radio" name="type" value="daily" onclick="updateScheduleForm()"> 毎日</label>
                             <label style="font-weight:normal; margin-top:2px; font-size:12px;"><input type="radio" name="type" value="weekly" onclick="updateScheduleForm()"> 毎週</label>
                             <label style="font-weight:normal; margin-top:2px; font-size:12px;"><input type="radio" name="type" value="hourly" onclick="updateScheduleForm()"> 毎時</label>
                             <label style="font-weight:normal; margin-top:2px; font-size:12px;"><input type="radio" name="type" value="interval" onclick="updateScheduleForm()"> 一定間隔</label>
                             <label style="font-weight:normal; margin-top:2px; font-size:12px;"><input type="radio" name="type" value="datetime" onclick="updateScheduleForm()"> 日時指定</label>
                         </div>

                         <label style="margin-top:3px;">日付 (日時指定用):</label>
                         <input type="date" name="run_date" id="input_run_date" style="width:100%; font-size:12px; padding:2px;">

                         <label style="margin-top:3px;">crontab設定 (例: <code>*/15 * * * *</code>):</label>
                         <input type="text" name="cron_expr" id="input_cron_expr" placeholder="*/15 * * * *" style="width:100%; font-size:12px; padding:2px;">

                         <label style="margin-top:3px;">曜日 (毎週用, 0=日, 1=月, ...):</label>
                         <input type="text" name="weekday" id="input_weekday" value="*" style="width:100%; font-size:12px; padding:2px;">

                         <label style="margin-top:3px;">開始時刻 (hh:mm):</label>
                         <div style="display: flex; gap: 5px; align-items: center;">
                             <input type="text" name="hour" id="input_hour" placeholder="hh" value="18" style="width: 40px; font-size:12px; padding:2px;"> : 
                             <input type="text" name="minute" id="input_minute" placeholder="mm" value="00" style="width: 40px; font-size:12px; padding:2px;">
                         </div>

                         <label style="margin-top:3px;">間隔時間 (毎時/一定間隔用・分):</label>
                         <input type="text" name="interval" id="input_interval" placeholder="15" style="width:100%; font-size:12px; padding:2px;">

                         <button type="submit" style="margin-top:8px; width:100%; padding:4px;">➕ 登録</button>
                     </form>
                 </div>
            </div>
        </div>

    {{else if or (eq .CurrentTab "nodes") (eq .CurrentTab "topology") (eq .CurrentTab "monitors") (eq .CurrentTab "nodes_manage") (eq .CurrentTab "node_new")}}
        <!-- ノード・監視管理 統合画面 -->
        <div class="split-layout">
            <!-- 1. ノード一覧セクション -->
            <div class="split-section" style="min-height: 300px; display: flex; flex-direction: row; gap: 5px; height: 350px;">
                 <div style="flex: 1.5; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;">
                         <span>🖥️ ノード一覧</span>
                         <a href="/?tab=nodes" class="refresh-icon">🔄 更新</a>
                     </div>
                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px;">
                         <thead>
                             <tr>
                                 <th>ノード名</th>
                                 <th>IPアドレス</th>
                                 <th>状態 (監視)</th>
                             </tr>
                         </thead>
                         <tbody>
                             {{range .Nodes}}
                                 <tr class="{{if eq .ID $.SelectedNodeID}}tree-active{{end}}">
                                     <td><a href="/?tab={{$.CurrentTab}}&s={{.ID}}">{{.Name}}</a></td>
                                     <td>{{.IPAddress}}</td>
                                     <td>
                                         {{if eq .Description "正常"}}<span class="badge status-success">正常</span>
                                         {{else if eq .Description "異常"}}<span class="badge status-danger">異常</span>
                                         {{else}}<span class="badge status-unknown">未実施 / 不明</span>{{end}}
                                     </td>
                                 </tr>
                             {{else}}
                                 <tr><td colspan="3">登録ノードがありません。</td></tr>
                             {{end}}
                         </tbody>
                     </table>

                     <h4 style="margin-top:20px;">📝 ノード一括登録・編集 (CSV形式)</h4>
                     <form action="/action" method="POST">
                         <input type="hidden" name="action" value="save_nodes_bulk">
                         <textarea name="nodes_csv" rows="6" style="width:100%; font-family:monospace; font-size:12px;">{{.NodeCsvText}}</textarea>
                         <div style="margin-top:5px;">
                             <button type="submit" class="btn btn-primary" style="font-size:12px; padding:3px 10px;">💾 ノード一括保存 (適用)</button>
                         </div>
                     </form>
                 </div>

                 <div style="flex: 1; padding: 10px; height: 100%; box-sizing: border-box; overflow-y: auto; font-size:12px;">
                     {{if .SelectedNodeData}}
                         <div class="view-title" style="margin: -10px -10px 10px -10px;"><span>ノード詳細: {{.SelectedNodeData.Name}}</span></div>
                         <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px; margin-bottom:10px;">
                             <tr><th>IPアドレス</th><td>{{.SelectedNodeData.IPAddress}}</td></tr>
                             <tr><th>プラットフォーム</th><td>{{.SelectedNodeData.Platform}}</td></tr>
                             <tr><th>説明</th><td>{{.SelectedNodeData.Description}}</td></tr>
                         </table>

                         <h5 style="margin:10px 0 5px 0;">このノードの監視設定</h5>
                         <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:11px;">
                             <thead>
                                 <tr><th>監視項目</th><th>閾値</th><th>状態</th></tr>
                             </thead>
                             <tbody>
                                 {{range .NodeMonitors}}
                                     <tr>
                                         <td>{{.Type}} ({{.Target}})</td>
                                         <td>{{.Operator}} {{.ThresholdValue}}</td>
                                         <td>
                                             {{if eq .LastStatus "正常"}}<span class="badge status-success">正常</span>
                                             {{else if eq .LastStatus "異常"}}<span class="badge status-danger">異常</span>
                                             {{else}}<span class="badge status-unknown">未判定</span>{{end}}
                                         </td>
                                     </tr>
                                 {{else}}
                                     <tr><td colspan="3">このノードに対する監視設定はありません。</td></tr>
                                 {{end}}
                             </tbody>
                         </table>

                         <div style="margin-top:15px; display:flex; gap:5px;">
                             <form action="/action" method="POST" style="margin:0;">
                                 <input type="hidden" name="action" value="delete_node">
                                 <input type="hidden" name="id" value="{{.SelectedNodeData.ID}}">
                                 <button type="submit" class="btn btn-danger" onclick="return confirm('本当にこのノードを削除しますか？紐づく監視設定も削除されます。');">🗑️ ノード削除</button>
                             </form>
                             <a href="/?tab={{$.CurrentTab}}" class="btn">クリア</a>
                         </div>
                     {{else}}
                         <div class="view-title" style="margin: -10px -10px 10px -10px;"><span>➕ 新規ノード登録</span></div>
                         <form action="/action" method="POST">
                             <input type="hidden" name="action" value="create_node">
                             
                             <label style="margin-top:3px;">ノード名:</label>
                             <input type="text" name="name" required placeholder="例: DBサーバ" style="width:100%; font-size:12px; padding:2px;">

                             <label style="margin-top:5px;">IPアドレス:</label>
                             <input type="text" name="ip_address" required placeholder="例: 192.168.1.10" style="width:100%; font-size:12px; padding:2px;">

                             <label style="margin-top:5px;">プラットフォーム:</label>
                             <select name="platform" style="width:100%; font-size:12px; padding:2px;">
                                 <option value="linux">Linux</option>
                                 <option value="windows">Windows</option>
                             </select>

                             <label style="margin-top:5px;">説明:</label>
                             <input type="text" name="description" placeholder="用途など" style="width:100%; font-size:12px; padding:2px;">

                             <button type="submit" style="margin-top:10px; width:100%; padding:4px;">➕ 登録</button>
                         </form>
                     {{end}}
                 </div>
            </div>

            <!-- 2. ノードマップセクション -->
            <div class="split-section" style="min-height: 200px; padding:10px; height: 250px; overflow-y: auto; box-sizing: border-box;">
                 <div class="view-title" style="margin: -10px -10px 10px -10px;">
                     <span>🗺️ ノードマップ (トポロジー)</span>
                     <a href="/?tab=nodes" class="refresh-icon">🔄 更新</a>
                 </div>
                 <div class="topology-map" style="margin-top:10px; font-size:12px;">
        Himenos-Manager (Local)
               │
               ├───────────────┐
               ▼               ▼
         [監視ノード一覧]
    {{range .Nodes}}
         ├── ● <span class="topo-node {{if eq .Description "正常"}}topo-ok{{else if eq .Description "異常"}}topo-err{{else}}topo-unknown{{end}}"><a href="/?tab={{$.CurrentTab}}&s={{.ID}}" style="color:inherit;">{{.Name}} ({{.IPAddress}}) [{{.Description}}]</a></span>
    {{else}}
         └── 登録ノードがありません。
    {{end}}
                 </div>
            </div>

            <!-- 3. 監視管理セクション -->
            <div class="split-section" style="min-height: 300px; display: flex; flex-direction: row; gap: 5px; height: 350px; overflow-y: auto;">
                 <div style="flex: 1.5; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;">
                         <span>🔍 監視設定一覧</span>
                         <a href="/?tab=nodes" class="refresh-icon">🔄 更新</a>
                     </div>
                     <div style="font-size:12px; margin-bottom:10px;">
                         <strong>全体ステータス:</strong> 
                         <span class="badge status-danger">異常: {{.RedCount}}</span>
                         <span class="badge status-success">正常: {{.GreenCount}}</span>
                         <span class="badge status-unknown">不明: {{.BlueCount}}</span>
                         (合計: {{.TotalCount}} 件)
                     </div>

                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px; margin-bottom:15px;">
                         <thead>
                             <tr>
                                 <th>対象ノード</th>
                                 <th>監視項目</th>
                                 <th>閾値</th>
                                 <th>最終状態</th>
                                 <th>操作</th>
                             </tr>
                         </thead>
                         <tbody>
                             {{range .NodeMonitors}}
                                 <tr>
                                     <td>{{.NodeID}}</td>
                                     <td>{{.Type}} ({{.Target}})</td>
                                     <td>{{.Operator}} {{.ThresholdValue}}</td>
                                     <td>
                                         {{if eq .LastStatus "正常"}}<span class="badge status-success">正常</span>
                                         {{else if eq .LastStatus "異常"}}<span class="badge status-danger">異常: {{.LastResultValue}}</span>
                                         {{else}}<span class="badge status-unknown">未判定</span>{{end}}
                                     </td>
                                     <td>
                                         <form action="/action" method="POST" style="display:inline;">
                                             <input type="hidden" name="action" value="delete_monitor">
                                             <input type="hidden" name="id" value="{{.ID}}">
                                             <button type="submit" class="btn btn-danger">削除</button>
                                         </form>
                                     </td>
                                 </tr>
                             {{else}}
                                 <tr><td colspan="5">監視設定が登録されていません。</td></tr>
                             {{end}}
                         </tbody>
                     </table>

                     <h4 style="margin-top:20px;">📜 監視アラート履歴</h4>
                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:11px;">
                         <thead>
                             <tr>
                                 <th>日時</th>
                                 <th>対象ノード</th>
                                 <th>項目</th>
                                 <th>判定結果</th>
                                 <th>値</th>
                             </tr>
                         </thead>
                         <tbody>
                             {{range .MonitorHistory}}
                                 <tr>
                                     <td>{{.Timestamp}}</td>
                                     <td>{{.NodeID}}</td>
                                     <td>{{.MonitorType}}</td>
                                     <td>
                                         {{if eq .Status "正常"}}<span class="badge status-success">正常</span>
                                         {{else}}<span class="badge status-danger">異常</span>{{end}}
                                     </td>
                                     <td>{{.Value}} (閾値: {{.Detail}})</td>
                                 </tr>
                             {{else}}
                                 <tr><td colspan="5">監視履歴はありません。</td></tr>
                             {{end}}
                         </tbody>
                     </table>
                 </div>

                 <div style="flex: 1; padding: 10px; height: 100%; box-sizing: border-box; overflow-y: auto; font-size:12px;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;"><span>➕ 監視設定追加</span></div>
                     <form action="/action" method="POST">
                         <input type="hidden" name="action" value="create_monitor">
                         
                         <label style="margin-top:3px;">対象ノード:</label>
                         <select name="node_id" style="width:100%; font-size:12px; padding:2px;">
                             {{range .Nodes}}
                                 <option value="{{.ID}}">{{.Name}} ({{.IPAddress}})</option>
                             {{end}}
                         </select>

                         <label style="margin-top:5px;">監視項目タイプ:</label>
                         <select name="type" style="width:100%; font-size:12px; padding:2px;">
                             <option value="ping">PING 応答監視 (死活監視)</option>
                             <option value="cpu">CPU 使用率監視 (%)</option>
                             <option value="memory">メモリ 使用率監視 (%)</option>
                             <option value="disk">ディスク 空き容量監視 (%)</option>
                             <option value="port">TCP ポート接続確認</option>
                         </select>

                         <label style="margin-top:5px;">ターゲット (ポート番号やディスクパスなど):</label>
                         <input type="text" name="target" placeholder="例: 80 や C: または /" style="width:100%; font-size:12px; padding:2px;">

                         <label style="margin-top:5px;">比較演算子:</label>
                         <select name="operator" style="width:100%; font-size:12px; padding:2px;">
                             <option value=">">閾値より大きい ( > )</option>
                             <option value="<">閾値より小さい ( < )</option>
                             <option value="==">等しい ( == )</option>
                             <option value="!=">等しくない ( != )</option>
                         </select>

                         <label style="margin-top:5px;">閾値数値 (PING監視の場合はタイムアウトms値):</label>
                         <input type="text" name="threshold_value" required value="80" style="width:100%; font-size:12px; padding:2px;">

                         <button type="submit" style="margin-top:10px; width:100%; padding:4px;">➕ 登録</button>
                     </form>
                 </div>
            </div>
        </div>

    {{else if eq .CurrentTab "settings"}}
        <!-- 設定ペイン -->
        <div class="pane-full">
            <div class="view-title"><span>環境構築 (通知設定 & バックアップ)</span></div>
            <div class="view-content">
                {{if .ErrorMessage}}
                    <div class="error-message">{{.ErrorMessage}}</div>
                {{end}}

                <div style="background:#e8f0fe; padding:15px; border:1px solid #ccc; margin-bottom:20px;">
                    <h3>💾 Himenos設定のインポート / エクスポート (YAML形式)</h3>
                    <div class="help-text">すべての設定を一括バックアップ・復元できます。</div>
                    <div style="margin-top:10px;">
                        <a href="/export" class="btn btn-primary">📤 設定のエクスポート (himenos_backup.yaml ダウンロード)</a>
                    </div>
                    <form action="/import" method="POST" enctype="multipart/form-data" style="margin-top:15px; border-top:1px solid #ccc; padding-top:10px;">
                        <label>📥 設定のインポート (YAMLファイルをアップロード):</label>
                        <input type="file" name="backup_file" required>
                        <button type="submit" style="margin-top:5px;">インポート実行</button>
                    </form>
                </div>

                <form action="/action" method="POST">
                    <input type="hidden" name="action" value="update_settings">

                    <h3>共通デフォルト通知設定</h3>
                    <label>デフォルトの通知方法:</label>
                    <select name="default_notify">
                        <option value="" {{if eq .Settings.DefaultNotify ""}}selected{{end}}>通知しない</option>
                        <option value="both" {{if eq .Settings.DefaultNotify "both"}}selected{{end}}>メール & Slack 両方</option>
                        <option value="slack" {{if eq .Settings.DefaultNotify "slack"}}selected{{end}}>Slack のみ</option>
                        <option value="email" {{if eq .Settings.DefaultNotify "email"}}selected{{end}}>メール のみ</option>
                    </select>
                    <div class="help-text">個別ジョブや監視で「デフォルト設定に従う」を選択した際、この設定が適用されます。</div>

                    <h3>Slack 通知設定</h3>
                    <label>Incoming Webhook URL:</label>
                    <input type="text" name="slack_webhook" value="{{.Settings.SlackWebhook}}" placeholder="https://hooks.slack.com/services/...">

                    <h3>メール通知設定 (SMTP)</h3>
                    <label>SMTP サーバホスト名 (空なら通知をシミュレートログ出力します):</label>
                    <input type="text" name="smtp_host" value="{{.Settings.SMTPHost}}" placeholder="smtp.example.com">

                    <label>SMTP ポート番号:</label>
                    <input type="text" name="smtp_port" value="{{.Settings.SMTPPort}}" placeholder="587">

                    <label>SMTP ユーザ名:</label>
                    <input type="text" name="smtp_user" value="{{.Settings.SMTPUser}}">

                    <label>SMTP パスワード:</label>
                    <input type="password" name="smtp_pass" value="{{.Settings.SMTPPass}}">

                    <label>送信元メールアドレス (From):</label>
                    <input type="text" name="smtp_from" value="{{.Settings.SMTPFrom}}" placeholder="sender@example.com">

                    <label>送信先メールアドレス (To):</label>
                    <input type="text" name="smtp_to" value="{{.Settings.SMTPTo}}" placeholder="receiver@example.com">

                    <button type="submit">💾 設定を保存</button>
                </form>
            </div>
        </div>
    {{end}}
</div>

<div class="status-bar">
    <span>接続先Himenosマネージャ(1/1)：ローカル(localhost)</span>
    <span>Himenos-Go v3.1 Web UI (w3m対応)</span>
</div>

</body>
</html>`

type TreeItem struct {
	Node   *JobNode
	Prefix string
}

type ScriptFileInfo struct {
	Path      string
	Name      string
	IsEnabled bool
	Prefix    string
}

type PageData struct {
	RedCount          int
	YellowCount       int
	GreenCount        int
	BlueCount         int
	TotalCount        int
	ScriptFiles       []ScriptFileInfo
	SelectedScriptEnabled bool
	ScriptContent     string
	SchedulesCronText string
	HistoryKeyword    string
	NodeCsvText       string
	CurrentTime       string
	CurrentTab        string
	JobTree           []TreeItem
	SelectedJobID     string
	SelectedNode      *JobNode
	WaitConditionsStr string
	NewNodeType       string
	ParentID          string
	Schedules         []Schedule
	UnitOptions       []*JobNode
	Sessions          []*JobSession
	SelectedSessionID string
	SelectedSession   *JobSession
	SessionNodes      []SessionNodeItem
	ShowLogNode       *NodeState
	Settings          Settings
	JobMap            map[string]*JobNode
	ErrorMessage      string
	Nodes             []*ManagedNode
	SelectedNodeID    string
	SelectedNodeData  *ManagedNode
	NodeMonitors      []*MonitorSetting
	MonitorHistory    []MonitorHistory
}

type SessionNodeItem struct {
	Node        *NodeState
	StatusLabel string
}

func main() {
	initEngine()
	StartScheduler()
	engine.StartMonitoring()

	tmpl := template.Must(template.New("index").Parse(htmlTemplate))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tab := r.URL.Query().Get("tab")
		if tab == "" {
			tab = "schedules"
		}

		engine.mu.Lock()
		defer engine.mu.Unlock()

		data := PageData{
			CurrentTime: time.Now().Format("2006-01-02 15:04:05"),
			CurrentTab:  tab,
			Settings:    engine.settings,
			Schedules:   engine.schedules,
			Sessions:    engine.sessions,
			JobMap:      engine.jobs,
			ErrorMessage: r.URL.Query().Get("err"),
		}

		// scripts/ フォルダのスキャン
		if files, err := os.ReadDir(scriptsDir); err == nil {
			var tempFiles []os.DirEntry
			for _, f := range files {
				if !f.IsDir() {
					tempFiles = append(tempFiles, f)
				}
			}
			for idx, f := range tempFiles {
				filePath := filepath.Join(scriptsDir, f.Name())
				name := f.Name()
				isEnabled := !strings.HasSuffix(name, ".disabled")
				
				prefix := "   ├─ "
				if idx == len(tempFiles)-1 {
					prefix = "   └─ "
				}

				data.ScriptFiles = append(data.ScriptFiles, ScriptFileInfo{
					Path:      filePath,
					Name:      name,
					IsEnabled: isEnabled,
					Prefix:    prefix,
				})
			}
		}

		// ジョブユニットツリーの構築 (w3m対応テキストツリー)
		var buildTree func(id string, indent string, isLast bool)
		buildTree = func(id string, indent string, isLast bool) {
			n := engine.jobs[id]
			if n == nil {
				return
			}
			prefix := indent
			if indent != "" {
				if isLast {
					prefix += "   └─ "
				} else {
					prefix += "   ├─ "
				}
			}
			data.JobTree = append(data.JobTree, TreeItem{Node: n, Prefix: prefix})

			nextIndent := indent
			if indent != "" {
				if isLast {
					nextIndent += "      "
				} else {
					nextIndent += "   │  "
				}
			} else {
				nextIndent = " "
			}

			for i, childID := range n.Children {
				buildTree(childID, nextIndent, i == len(n.Children)-1)
			}
		}

		// 最上位（親なし）のユニットを探索
		var rootUnits []string
		for id, n := range engine.jobs {
			if n.Type == TypeUnit && (n.ParentID == "" || n.ParentID == "trash") {
				rootUnits = append(rootUnits, id)
			}
		}
		for _, rootID := range rootUnits {
			buildTree(rootID, "", true)
		}

		// (ゴミ箱ノード構築処理は削除)

		// ジョブ詳細
		isJobTab := tab == "jobs" || tab == "job_edit" || tab == "job_new" || tab == "history" || tab == "schedules" || tab == "script_edit" || tab == "jobs_manage"
		isNodeTab := tab == "nodes" || tab == "topology" || tab == "monitors" || tab == "nodes_manage" || tab == "node_new"

		if isJobTab {
			// 1. ジョブ設定データロード
			selected := r.URL.Query().Get("s")
			if selected != "" {
				if node, exists := engine.jobs[selected]; exists {
					data.SelectedJobID = selected
					data.SelectedNode = node
				}
			}

			id := r.URL.Query().Get("id")
			if id != "" {
				if node, exists := engine.jobs[id]; exists {
					data.SelectedNode = node
					var conds []string
					for _, c := range node.WaitConditions {
						targetNode := engine.jobs[c.JobID]
						if targetNode != nil {
							conds = append(conds, targetNode.Name)
						} else {
							conds = append(conds, c.JobID)
						}
					}
					data.WaitConditionsStr = strings.Join(conds, ", ")

					if node.Type == TypeJob && node.Command != "" {
						scriptPath := node.Command
						if scriptData, err := os.ReadFile(scriptPath); err == nil {
							data.ScriptContent = string(scriptData)
						}
					}
				}
			}

			if tab == "job_new" {
				data.NewNodeType = r.URL.Query().Get("type")
				data.ParentID = r.URL.Query().Get("parent")
				data.SelectedJobID = r.URL.Query().Get("script_preselect")
			}

			// 2. スクリプト直接編集データロード
			filePath := r.URL.Query().Get("file")
			if filePath != "" {
				data.SelectedJobID = filePath
				if scriptData, err := os.ReadFile(filePath); err == nil {
					data.ScriptContent = string(scriptData)
				}
				data.SelectedScriptEnabled = !strings.HasSuffix(filePath, ".disabled")
				for _, node := range engine.jobs {
					if node.Type == TypeJob && node.Command == filePath {
						data.SelectedNode = node
						break
					}
				}
			}

			// 3. スケジュール設定データロード
			for _, n := range engine.jobs {
				if n.Type == TypeUnit || n.Type == TypeJob || n.Type == TypeNet {
					data.UnitOptions = append(data.UnitOptions, n)
				}
			}
			var cronLines []string
			for _, s := range engine.schedules {
				expr := s.CronExpr
				if expr == "" {
					min := s.Minute
					if min == "" || min == "*" { min = "0" }
					hour := s.Hour
					if hour == "" || hour == "*" { hour = "0" }
					dom := s.Day
					if dom == "" { dom = "*" }
					mon := s.Month
					if mon == "" { mon = "*" }
					dow := s.Weekday
					if dow == "" { dow = "*" }

					if s.Type == "hourly" {
						expr = fmt.Sprintf("*/%d * * * *", s.Interval)
					} else if s.Type == "interval" {
						expr = fmt.Sprintf("*/%d * * * *", s.Interval)
					} else {
						expr = fmt.Sprintf("%s %s %s %s %s", min, hour, dom, mon, dow)
					}
				}
				cronLines = append(cronLines, fmt.Sprintf("%s %s # %s", expr, s.JobID, s.Name))
			}
			data.SchedulesCronText = strings.Join(cronLines, "\n")

			// 4. ジョブ履歴データロード
			keyword := r.URL.Query().Get("keyword")
			startDateStr := r.URL.Query().Get("start_date")
			endDateStr := r.URL.Query().Get("end_date")
			filterStatus := r.URL.Query().Get("filter_status")

			data.HistoryKeyword = keyword
			data.SelectedJobID = startDateStr
			data.WaitConditionsStr = endDateStr

			var filteredSessions []*JobSession
			for _, s := range engine.sessions {
				if keyword != "" {
					if !strings.Contains(s.UnitName, keyword) && !strings.Contains(s.SessionID, keyword) {
						continue
					}
				}
				if startDateStr != "" {
					if s.StartDate < startDateStr { continue }
				}
				if endDateStr != "" {
					if s.StartDate > endDateStr + " 23:59:59" { continue }
				}
				if filterStatus != "" {
					if filterStatus == "実行中" {
						if s.Status != "実行中" { continue }
					} else {
						if s.Status != filterStatus { continue }
					}
				}
				filteredSessions = append(filteredSessions, s)
			}
			data.Sessions = filteredSessions

			var red, yellow, green, blue int
			sessionID := r.URL.Query().Get("session_id")
			if sessionID != "" {
				for _, s := range engine.sessions {
					if s.SessionID == sessionID {
						data.SelectedSessionID = sessionID
						data.SelectedSession = s

						var collectSessionNodes func(id string)
						collectSessionNodes = func(id string) {
							nState, exists := s.Nodes[id]
							if !exists { return }
							nodeDef := engine.jobs[id]
							statusLabel := nState.Status
							if nState.Status == "終了" && nodeDef != nil {
								statusLabel = engine.determineStatusFromRange(nodeDef, nState.ExitValue)
							}
							data.SessionNodes = append(data.SessionNodes, SessionNodeItem{
								Node:        nState,
								StatusLabel: statusLabel,
							})
							if nodeDef != nil {
								for _, childID := range nodeDef.Children {
									collectSessionNodes(childID)
								}
							}
						}
						collectSessionNodes(s.UnitID)

						showLogJobID := r.URL.Query().Get("show_log")
						if showLogJobID != "" {
							if ns, exists := s.Nodes[showLogJobID]; exists {
								data.ShowLogNode = ns
							}
						}
						break
					}
				}
				for _, sn := range data.SessionNodes {
					nState := sn.Node
					statusName := sn.StatusLabel
					if statusName == "異常終了" || nState.Status == "起動失敗" {
						red++
					} else if statusName == "警告終了" {
						yellow++
					} else if statusName == "正常終了" || statusName == "終了" || nState.Status == "スキップ" {
						green++
					} else {
						blue++
					}
				}
				data.TotalCount = len(data.SessionNodes)
			} else {
				for _, s := range filteredSessions {
					if s.Status == "異常終了" {
						red++
					} else if s.Status == "警告終了" {
						yellow++
					} else if s.Status == "正常終了" {
						green++
					} else {
						blue++
					}
				}
				data.TotalCount = len(filteredSessions)
			}
			data.RedCount = red
			data.YellowCount = yellow
			data.GreenCount = green
			data.BlueCount = blue

		} else if isNodeTab {
			// 1. ノード一覧データロード
			id := r.URL.Query().Get("s")
			for _, n := range engine.nodes {
				data.Nodes = append(data.Nodes, n)
			}
			var lines []string
			for _, n := range engine.nodes {
				lines = append(lines, fmt.Sprintf("%s,%s,%s,%s", n.Name, n.IPAddress, n.Platform, n.Description))
			}
			data.NodeCsvText = strings.Join(lines, "\n")

			if id != "" {
				if n, exists := engine.nodes[id]; exists {
					data.SelectedNodeID = id
					data.SelectedNodeData = n
					for _, m := range engine.monitors {
						if m.NodeID == id {
							data.NodeMonitors = append(data.NodeMonitors, m)
						}
					}
				}
			}

			// 2. ノードマップ(トポロジー)＆監視最悪ステータス集計
			data.Nodes = nil // 一旦クリアして重複を防ぐ
			for _, n := range engine.nodes {
				worstStatus := "正常"
				hasMonitor := false
				for _, m := range engine.monitors {
					if m.NodeID == n.ID {
						hasMonitor = true
						if m.LastStatus == "異常" {
							worstStatus = "異常"
						} else if m.LastStatus == "未実施" && worstStatus == "正常" {
							worstStatus = "未実施"
						}
					}
				}
				if !hasMonitor {
					worstStatus = "未実施"
				}
				n.Description = worstStatus
				data.Nodes = append(data.Nodes, n)
			}

			// 3. 監視管理データロード
			data.MonitorHistory = engine.monHistory
			for _, m := range engine.monitors {
				data.NodeMonitors = append(data.NodeMonitors, m)
			}
			var red, green, blue int
			for _, m := range engine.monitors {
				if m.LastStatus == "異常" {
					red++
				} else if m.LastStatus == "正常" {
					green++
				} else {
					blue++
				}
			}
			data.RedCount = red
			data.GreenCount = green
			data.BlueCount = blue
			data.TotalCount = len(engine.monitors)
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = tmpl.Execute(w, data)
	})

	http.HandleFunc("/export", func(w http.ResponseWriter, r *http.Request) {
		engine.mu.Lock()
		var jobList []*JobNode
		for _, j := range engine.jobs {
			jobList = append(jobList, j)
		}
		backup := BackupData{
			Jobs:      jobList,
			Schedules: engine.schedules,
			Settings:  engine.settings,
		}
		for _, n := range engine.nodes {
			backup.Nodes = append(backup.Nodes, n)
		}
		for _, m := range engine.monitors {
			backup.Monitors = append(backup.Monitors, m)
		}
		engine.mu.Unlock()

		data, err := yaml.Marshal(backup)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-yaml")
		w.Header().Set("Content-Disposition", "attachment; filename=himenos_backup.yaml")
		w.Write(data)
	})

	http.HandleFunc("/import", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/?tab=settings", http.StatusSeeOther)
			return
		}
		file, _, err := r.FormFile("backup_file")
		if err != nil {
			http.Redirect(w, r, "/?tab=settings&err=" + err.Error(), http.StatusSeeOther)
			return
		}
		defer file.Close()

		yamlData, err := io.ReadAll(file)
		if err != nil {
			http.Redirect(w, r, "/?tab=settings&err=" + err.Error(), http.StatusSeeOther)
			return
		}

		var backup BackupData
		if err := yaml.Unmarshal(yamlData, &backup); err != nil {
			http.Redirect(w, r, "/?tab=settings&err=YAMLファイルの解析に失敗しました: " + err.Error(), http.StatusSeeOther)
			return
		}

		engine.mu.Lock()
		if backup.Jobs != nil {
			engine.jobs = make(map[string]*JobNode)
			for _, j := range backup.Jobs {
				engine.jobs[j.ID] = j
			}
			engine.saveJobs()
		}
		if backup.Schedules != nil {
			engine.schedules = backup.Schedules
			engine.saveSchedules()
		}
		engine.settings = backup.Settings
		engine.saveSettings()

		if backup.Nodes != nil {
			engine.nodes = make(map[string]*ManagedNode)
			for _, n := range backup.Nodes {
				engine.nodes[n.ID] = n
			}
			engine.saveNodes()
		}
		if backup.Monitors != nil {
			engine.monitors = make(map[string]*MonitorSetting)
			for _, m := range backup.Monitors {
				engine.monitors[m.ID] = m
			}
			engine.saveMonitors()
		}
		engine.mu.Unlock()

		http.Redirect(w, r, "/?tab=settings&err=設定インポートが成功しました。", http.StatusSeeOther)
	})

	http.HandleFunc("/action", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		action := r.FormValue("action")
		redirectTo := "/"

		switch action {
						case "create_job":
			nodeType := JobType(r.FormValue("type"))
			parentID := r.FormValue("parent_id")
			name := r.FormValue("name")
			if name == "" {
				scriptSelect := r.FormValue("script_select")
				if scriptSelect == "__NEW__" {
					name = r.FormValue("script_filename")
				} else if scriptSelect != "" {
					name = filepath.Base(scriptSelect)
				}
				if name == "" {
					name = "新規ジョブ"
				}
			}

			// IDを自動生成
			id := "job_" + fmt.Sprintf("%d", time.Now().UnixNano())

			// 簡素化された通知
			notifyVal := r.FormValue("notify_all")
			if notifyVal == "" {
				notifyVal = "default"
			}

			node := &JobNode{
				ID:            id,
				Name:          name,
				Type:          nodeType,
				ParentID:      parentID,
				NormalRange:   "0",
				WaitRelation:  "AND",
				NotifyStart:   notifyVal,
				NotifyNormal:  notifyVal,
				NotifyWarning: notifyVal,
				NotifyError:   notifyVal,
			}

			if nodeType == TypeJob {
				node.RunUser = r.FormValue("run_user") // 引数

				scriptSelect := r.FormValue("script_select")
				if scriptSelect == "__NEW__" {
					filename := r.FormValue("script_filename")
					content := r.FormValue("script_content")

					// バリデーション: ファイル名または本文が空なら作成不可
					if filename == "" || content == "" {
						redirectTo = fmt.Sprintf("/?tab=job_new&type=job&parent=%s&err=%s", parentID, "新規ジョブにはスクリプトファイル名と本文の入力が必須です。")
						http.Redirect(w, r, redirectTo, http.StatusSeeOther)
						return
					}

					if !strings.HasSuffix(filename, ".sh") && !strings.HasSuffix(filename, ".bat") {
						filename += ".sh"
					}
					scriptPath := filepath.Join(scriptsDir, filename)
					_ = os.WriteFile(scriptPath, []byte(content), 0755)
					gitCommit("Web: Create script " + filename, scriptPath)
					node.Command = scriptPath
				} else {
					// 既存のスクリプトを選択。これも空ならエラー
					if scriptSelect == "" {
						redirectTo = fmt.Sprintf("/?tab=job_new&type=job&parent=%s&err=%s", parentID, "実行するスクリプトを指定してください。")
						http.Redirect(w, r, redirectTo, http.StatusSeeOther)
						return
					}
					node.Command = scriptSelect
				}
			}

			// 待ち条件のパース (ジョブ名からIDへ逆解決)
			waitStr := r.FormValue("wait_conditions")
			if waitStr != "" {
				parts := strings.Split(waitStr, ",")
				engine.mu.Lock()
				for _, p := range parts {
					jName := strings.TrimSpace(p)
					if jName == "" {
						continue
					}
					// 名前に一致するジョブを探す
					foundID := ""
					for _, existing := range engine.jobs {
						if existing.Name == jName {
							foundID = existing.ID
							break
						}
					}
					if foundID != "" {
						node.WaitConditions = append(node.WaitConditions, WaitCondition{
							Type:  "status",
							JobID: foundID,
							Value: "正常",
						})
					}
				}
				engine.mu.Unlock()
			}

			engine.mu.Lock()
			engine.jobs[id] = node
			if parentID != "" {
				if parent, exists := engine.jobs[parentID]; exists {
					parent.Children = append(parent.Children, id)
				}
			}
			engine.saveJobs()
			engine.mu.Unlock()
			redirectTo = "/?tab=jobs&s=" + id

						case "update_job":
			id := r.FormValue("id")
			engine.mu.Lock()
			node, exists := engine.jobs[id]
			if exists {
				node.Name = r.FormValue("name")
				
				notifyVal := r.FormValue("notify_all")
				if notifyVal == "" {
					notifyVal = "default"
				}
				node.NotifyStart = notifyVal
				node.NotifyNormal = notifyVal
				node.NotifyWarning = notifyVal
				node.NotifyError = notifyVal

				if node.Type == TypeJob {
					node.RunUser = r.FormValue("run_user")

					// スクリプト編集
					if node.Command != "" {
						scriptContent := r.FormValue("script_content")
						if scriptContent == "" {
							engine.mu.Unlock()
							redirectTo = fmt.Sprintf("/?tab=job_edit&id=%s&err=%s", id, "スクリプト本文を空にすることはできません。")
							http.Redirect(w, r, redirectTo, http.StatusSeeOther)
							return
						}
						_ = os.WriteFile(node.Command, []byte(scriptContent), 0755)
						gitCommit("Web: Update script content for " + id, node.Command)
					}
				}

				// 待ち条件の更新 (ジョブ名からIDへ逆解決)
				node.WaitConditions = nil
				waitStr := r.FormValue("wait_conditions")
				if waitStr != "" {
					parts := strings.Split(waitStr, ",")
					for _, p := range parts {
						jName := strings.TrimSpace(p)
						if jName == "" {
							continue
						}
						foundID := ""
						for _, existing := range engine.jobs {
							if existing.Name == jName {
								foundID = existing.ID
								break
							}
						}
						if foundID != "" {
							node.WaitConditions = append(node.WaitConditions, WaitCondition{
								Type:  "status",
								JobID: foundID,
								Value: "正常",
							})
						}
					}
				}
				engine.saveJobs()
			}
			engine.mu.Unlock()
			redirectTo = "/?tab=jobs&s=" + id

		case "delete_job":
			id := r.FormValue("id")
			engine.mu.Lock()
			var purgeRecursive func(nid string)
			purgeRecursive = func(nid string) {
				if n, exists := engine.jobs[nid]; exists {
					for _, cid := range n.Children {
						purgeRecursive(cid)
					}
					delete(engine.jobs, nid)
				}
			}
			if node, exists := engine.jobs[id]; exists {
				if node.ParentID != "" {
					if parent, pExists := engine.jobs[node.ParentID]; pExists {
						var newChildren []string
						for _, cid := range parent.Children {
							if cid != id {
								newChildren = append(newChildren, cid)
							}
						}
						parent.Children = newChildren
					}
				}
			}
			purgeRecursive(id)
			engine.saveJobs()
			engine.mu.Unlock()
			redirectTo = "/?tab=jobs"

		case "toggle_script":
			filePath := r.FormValue("file")
			if !strings.HasPrefix(filepath.ToSlash(filePath), "scripts/") {
				redirectTo = "/?tab=script_edit&file=" + filePath
				http.Redirect(w, r, redirectTo, http.StatusSeeOther)
				return
			}
			
			if strings.HasSuffix(filePath, ".disabled") {
				newPath := strings.TrimSuffix(filePath, ".disabled")
				_ = os.Rename(filePath, newPath)
				gitCommit("Web: Enable script " + filepath.Base(newPath), newPath)
				redirectTo = "/?tab=script_edit&file=" + newPath
			} else {
				newPath := filePath + ".disabled"
				_ = os.Rename(filePath, newPath)
				gitCommit("Web: Disable script " + filepath.Base(filePath), newPath)
				redirectTo = "/?tab=script_edit&file=" + newPath
			}

		case "run_job":
			id := r.FormValue("id")
			sessionID := engine.StartSession(id, "手動実行", "Admin")
			redirectTo = "/?tab=history&session_id=" + sessionID

		case "stop_node":
			sessionID := r.FormValue("session_id")
			jobID := r.FormValue("job_id")
			control := r.FormValue("control") // "コマンド", "保留解除" などのオペレーション
			engine.StopNode(sessionID, jobID, control)
			redirectTo = "/?tab=history&session_id=" + sessionID

		case "create_schedule":
			jobID := r.FormValue("job_id")
			schedID := "sched_" + jobID + "_" + fmt.Sprintf("%d", time.Now().Unix())
			intervalVal, _ := strconv.Atoi(r.FormValue("interval"))

			monthVal := "*"
			dayVal := "*"
			runDateStr := r.FormValue("run_date")
			if runDateStr != "" {
				if parts := strings.Split(runDateStr, "-"); len(parts) == 3 {
					m, _ := strconv.Atoi(parts[1])
					d, _ := strconv.Atoi(parts[2])
					monthVal = fmt.Sprintf("%d", m)
					dayVal = fmt.Sprintf("%d", d)
				}
			}

			sched := Schedule{
				ID:       schedID,
				Name:     r.FormValue("name"),
				JobID:    jobID,
				Type:     r.FormValue("type"),
				Month:    monthVal,
				Day:      dayVal,
				Weekday:  r.FormValue("weekday"),
				CronExpr: r.FormValue("cron_expr"),
				Hour:     r.FormValue("hour"),
				Minute:   r.FormValue("minute"),
				Interval: intervalVal,
				Enabled:  true,
			}
			engine.mu.Lock()
			engine.schedules = append(engine.schedules, sched)
			engine.saveSchedules()
			engine.mu.Unlock()
			redirectTo = "/?tab=schedules"

		case "toggle_schedule":
			id := r.FormValue("id")
			engine.mu.Lock()
			for i, s := range engine.schedules {
				if s.ID == id {
					engine.schedules[i].Enabled = !engine.schedules[i].Enabled
					break
				}
			}
			engine.saveSchedules()
			engine.mu.Unlock()
			redirectTo = "/?tab=schedules"

		case "delete_schedule":
			id := r.FormValue("id")
			engine.mu.Lock()
			var newScheds []Schedule
			for _, s := range engine.schedules {
				if s.ID != id {
					newScheds = append(newScheds, s)
				}
			}
			engine.schedules = newScheds
			engine.saveSchedules()
			engine.mu.Unlock()
			redirectTo = "/?tab=schedules"

		case "save_schedules_bulk":
			schedulesCron := r.FormValue("schedules_cron")
			engine.mu.Lock()

			var newScheds []Schedule
			lines := strings.Split(schedulesCron, "\n")

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				enabled := true
				if strings.HasPrefix(line, "#") {
					enabled = false
					line = strings.TrimSpace(strings.TrimPrefix(line, "#"))
					if line == "" {
						continue
					}
				}

				var schedName string
				var mainPart string
				if idx := strings.LastIndex(line, "#"); idx != -1 {
					mainPart = strings.TrimSpace(line[:idx])
					schedName = strings.TrimSpace(line[idx+1:])
				} else {
					mainPart = line
					schedName = "一括登録スケジュール"
				}

				fields := strings.Fields(mainPart)
				if len(fields) < 6 {
					continue
				}

				cronExpr := strings.Join(fields[0:5], " ")
				jobID := fields[5]

				var schedID string
				for _, oldSched := range engine.schedules {
					if oldSched.JobID == jobID && oldSched.CronExpr == cronExpr {
						schedID = oldSched.ID
						break
					}
				}
				if schedID == "" {
					schedID = "sched_" + jobID + "_" + fmt.Sprintf("%d", time.Now().UnixNano())
				}

				newScheds = append(newScheds, Schedule{
					ID:       schedID,
					Name:     schedName,
					JobID:    jobID,
					Type:     "cron",
					CronExpr: cronExpr,
					Enabled:  enabled,
				})
			}

			engine.schedules = newScheds
			engine.saveSchedules()
			engine.mu.Unlock()
			redirectTo = "/?tab=schedules"

		case "create_node":
			name := r.FormValue("name")
			ip := r.FormValue("ip_address")
			platform := r.FormValue("platform")
			desc := r.FormValue("description")
			id := "node_" + fmt.Sprintf("%d", time.Now().UnixNano())

			node := &ManagedNode{
				ID:          id,
				Name:        name,
				Platform:    platform,
				IPAddress:   ip,
				Description: desc,
			}
			engine.mu.Lock()
			engine.nodes[id] = node
			engine.saveNodes()

			// デフォルトPing監視の自動追加
			monitorID := "mon_ping_" + id
			monitor := &MonitorSetting{
				ID:         monitorID,
				Name:       "Ping監視",
				NodeID:     id,
				Type:       "ping",
				Target:     ip,
				Enabled:    true,
				LastStatus: "未実施",
			}
			engine.monitors[monitorID] = monitor
			engine.saveMonitors()
			engine.mu.Unlock()
			redirectTo = "/?tab=nodes&s=" + id

		case "save_nodes_bulk":
			nodesCSV := r.FormValue("nodes_csv")
			engine.mu.Lock()

			newNodes := make(map[string]*ManagedNode)
			lines := strings.Split(nodesCSV, "\n")
			nodeCounter := int64(0)

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}

				parts := strings.Split(line, ",")
				if len(parts) < 2 {
					parts = strings.Split(line, "，")
				}

				var name, ipOrCIDR string
				var platform string = "LINUX"
				var desc string = ""

				if len(parts) >= 2 {
					name = strings.TrimSpace(parts[0])
					ipOrCIDR = strings.TrimSpace(parts[1])
					if len(parts) >= 3 {
						platform = strings.ToUpper(strings.TrimSpace(parts[2]))
						if platform != "LINUX" && platform != "WINDOWS" && platform != "OTHER" {
							platform = "LINUX"
						}
					}
					if len(parts) >= 4 {
						desc = strings.TrimSpace(parts[3])
					}
				} else if len(parts) == 1 {
					// IPアドレス（またはCIDR）のみとみなす
					ipOrCIDR = strings.TrimSpace(parts[0])
					name = ""
				}

				if ipOrCIDR == "" {
					continue
				}

				// IPOrCIDRがCIDR形式か確認して展開
				var ipList []string
				if strings.Contains(ipOrCIDR, "/") {
					ipList = expandCIDR(ipOrCIDR)
				}

				if len(ipList) > 0 {
					// CIDR展開登録
					for _, expandedIP := range ipList {
						var nodeName string
						if name == "" {
							nodeName = expandedIP
						} else {
							nodeName = fmt.Sprintf("%s_%s", name, expandedIP)
						}

						var nodeID string
						exists := false
						for oldID, oldNode := range engine.nodes {
							if oldNode.IPAddress == expandedIP || oldNode.Name == nodeName {
								nodeID = oldID
								exists = true
								break
							}
						}

						if !exists {
							nodeID = fmt.Sprintf("node_%d_%d", time.Now().UnixNano(), nodeCounter)
							nodeCounter++
							
							monitorID := "mon_ping_" + nodeID
							monitor := &MonitorSetting{
								ID:         monitorID,
								Name:       "Ping監視",
								NodeID:     nodeID,
								Type:       "ping",
								Target:     expandedIP,
								Enabled:    true,
								LastStatus: "未実施",
							}
							engine.monitors[monitorID] = monitor
						}

						newNodes[nodeID] = &ManagedNode{
							ID:          nodeID,
							Name:        nodeName,
							Platform:    platform,
							IPAddress:   expandedIP,
							Description: desc,
						}
					}
				} else {
					// 単一のIPアドレス登録
					if name == "" {
						name = ipOrCIDR
					}

					var nodeID string
					exists := false
					for oldID, oldNode := range engine.nodes {
						if oldNode.IPAddress == ipOrCIDR || oldNode.Name == name {
							nodeID = oldID
							exists = true
							break
						}
					}

					if !exists {
						nodeID = fmt.Sprintf("node_%d_%d", time.Now().UnixNano(), nodeCounter)
						nodeCounter++
						
						monitorID := "mon_ping_" + nodeID
						monitor := &MonitorSetting{
							ID:         monitorID,
							Name:       "Ping監視",
							NodeID:     nodeID,
							Type:       "ping",
							Target:     ipOrCIDR,
							Enabled:    true,
							LastStatus: "未実施",
						}
						engine.monitors[monitorID] = monitor
					}

					newNodes[nodeID] = &ManagedNode{
						ID:          nodeID,
						Name:        name,
						Platform:    platform,
						IPAddress:   ipOrCIDR,
						Description: desc,
					}
				}
			}

			// 削除されたノードに関連する監視設定を除去
			for mID, m := range engine.monitors {
				if _, exists := newNodes[m.NodeID]; !exists {
					delete(engine.monitors, mID)
				}
			}

			engine.nodes = newNodes
			engine.saveNodes()
			engine.saveMonitors()
			engine.mu.Unlock()
			redirectTo = "/?tab=nodes"

		case "toggle_monitor":
			id := r.FormValue("id")
			nodeID := r.FormValue("node_id")
			engine.mu.Lock()
			if m, exists := engine.monitors[id]; exists {
				m.Enabled = !m.Enabled
				engine.saveMonitors()
			}
			engine.mu.Unlock()
			redirectTo = "/?tab=nodes&s=" + nodeID

		case "delete_node":
			id := r.FormValue("id")
			engine.mu.Lock()
			delete(engine.nodes, id)
			// 関連する監視も削除
			for mID, m := range engine.monitors {
				if m.NodeID == id {
					delete(engine.monitors, mID)
				}
			}
			engine.saveNodes()
			engine.saveMonitors()
			engine.mu.Unlock()
			redirectTo = "/?tab=nodes"

		case "create_monitor":
			nodeID := r.FormValue("node_id")
			name := r.FormValue("name")
			mType := r.FormValue("type")
			target := r.FormValue("target")
			id := "mon_" + fmt.Sprintf("%d", time.Now().UnixNano())

			if mType == "ping" {
				// pingの場合は自動的にノードのIPアドレスを使うのでtargetは空でもよいが、記録用にセット
				if node, exists := engine.nodes[nodeID]; exists {
					target = node.IPAddress
				}
			}

			m := &MonitorSetting{
				ID:         id,
				Name:       name,
				NodeID:     nodeID,
				Type:       mType,
				Target:     target,
				Enabled:    true,
				LastStatus: "未実施",
			}
			engine.mu.Lock()
			engine.monitors[id] = m
			engine.saveMonitors()
			engine.mu.Unlock()
			redirectTo = "/?tab=nodes&s=" + nodeID

		case "delete_monitor":
			id := r.FormValue("id")
			nodeID := r.FormValue("node_id")
			engine.mu.Lock()
			delete(engine.monitors, id)
			engine.saveMonitors()
			engine.mu.Unlock()
			redirectTo = "/?tab=nodes&s=" + nodeID

		case "save_script_only":
			filePath := r.FormValue("filepath")
			scriptContent := r.FormValue("script_content")
			_ = os.WriteFile(filePath, []byte(scriptContent), 0755)
			gitCommit("Web: Save script " + filePath, filePath)
			redirectTo = "/?tab=script_edit&file=" + filePath

		case "update_settings":
			engine.mu.Lock()
			engine.settings.DefaultNotify = r.FormValue("default_notify")
			engine.settings.SlackWebhook = r.FormValue("slack_webhook")
			engine.settings.SMTPHost = r.FormValue("smtp_host")
			engine.settings.SMTPPort = r.FormValue("smtp_port")
			engine.settings.SMTPUser = r.FormValue("smtp_user")
			engine.settings.SMTPPass = r.FormValue("smtp_pass")
			engine.settings.SMTPFrom = r.FormValue("smtp_from")
			engine.settings.SMTPTo = r.FormValue("smtp_to")
			engine.saveSettings()
			engine.mu.Unlock()
			redirectTo = "/?tab=settings"
		}

		http.Redirect(w, r, redirectTo, http.StatusSeeOther)
	})

	fmt.Println("Server started on :8080")
	_ = http.ListenAndServe(":8080", nil)
}

func expandCIDR(cidr string) []string {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return nil
	}

	var ips []string
	ones, bits := ipnet.Mask.Size()
	if bits != 32 {
		return nil
	}

	// 負荷保護のため /22（1024ホスト）以上の大きさのサブネットは除外する
	if ones < 22 {
		return nil
	}

	startIP := ip4ToUint32(ipnet.IP)
	numHosts := 1 << (32 - ones)

	for i := 0; i < numHosts; i++ {
		currentIP := uint32ToIP4(startIP + uint32(i))
		// /31 や /32 以外の場合、ネットワークアドレスとブロードキャストアドレスを除外
		if numHosts > 2 {
			if i == 0 || i == numHosts-1 {
				continue
			}
		}
		ips = append(ips, currentIP.String())
	}
	return ips
}

func ip4ToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	return uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
}

func uint32ToIP4(val uint32) net.IP {
	return net.IPv4(byte(val>>24), byte(val>>16), byte(val>>8), byte(val))
}
