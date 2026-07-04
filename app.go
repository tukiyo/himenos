package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
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

type BasicAuthUser struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Settings struct {
	SMTPHost             string          `yaml:"smtp_host"`
	SMTPPort             string          `yaml:"smtp_port"`
	SMTPUser             string          `yaml:"smtp_user"`
	SMTPPass             string          `yaml:"smtp_pass"`
	SMTPFrom             string          `yaml:"smtp_from"`
	SMTPTo               string          `yaml:"smtp_to"`
	SlackWebhook         string          `yaml:"slack_webhook"`
	DefaultNotify        string          `yaml:"default_notify"` // デフォルト通知設定
	BasicAuthUsers       []BasicAuthUser `yaml:"basic_auth_users"`
	AllowedIPs           []string        `yaml:"allowed_ips"`
	IPRestrictionEnabled bool            `yaml:"ip_restriction_enabled"`
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
	ID              string `yaml:"id"`
	Name            string `yaml:"name"`
	NodeID          string `yaml:"node_id"`
	Type            string `yaml:"type"` // "ping", "http", "port"
	Target          string `yaml:"target"`
	Operator        string `yaml:"operator"`
	ThresholdValue  string `yaml:"threshold_value"`
	LastStatus      string `yaml:"last_status"`
	LastCheck       string `yaml:"last_check"`
	LastResultValue string `yaml:"last_result_value"`
	Enabled         bool   `yaml:"enabled"`
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
        :root {
            --bg-color: #f0f0f0;
            --panel-bg: #ffffff;
            --text-color: #000000;
            --border-color: #b0b0b0;
            --header-bg: linear-gradient(to bottom, #e1e9f6, #c5d7ed);
            --header-text: #1e395b;
            --toolbar-bg: #f5f5f5;
            --status-bar-bg: #e1e1e1;
            --card-bg: #ffffff;
            --alert-bg: #fafafa;
            --btn-hover: #e5e5e5;
            --btn-active: #ffffff;
            --input-bg: #ffffff;
        }
        
        [data-theme="dark"] button, [data-theme="dark"] .btn {
            background: linear-gradient(to bottom, #3a3a3a, #2d2d2d) !important;
            border-color: #555 !important;
            color: #e0e0e0 !important;
        }
        [data-theme="dark"] input, [data-theme="dark"] select, [data-theme="dark"] textarea {
            background: #2d2d2d !important;
            color: #e0e0e0 !important;
            border: 1px solid #555 !important;
        }

        [data-theme="dark"] {
            --bg-color: #121212;
            --panel-bg: #1e1e1e;
            --text-color: #e0e0e0;
            --border-color: #3d3d3d;
            --header-bg: linear-gradient(to bottom, #2b3a4a, #1a2535);
            --header-text: #ecf0f5;
            --toolbar-bg: #181818;
            --status-bar-bg: #181818;
            --card-bg: #282828;
            --alert-bg: #242424;
            --btn-hover: #333333;
            --btn-active: #1e1e1e;
            --input-bg: #2d2d2d;
        }
        /* 全体スタイル - Windowsクラシック / Eclipse RCP風 */
        body {
            font-family: 'MS PGothic', 'Meiryo', sans-serif;
            background: var(--bg-color);
            color: var(--text-color);
            margin: 0;
            padding: 0;
            font-size: 12px;
        }

        /* ツールバー (タブ・パースペクティブ) */
        .toolbar {
            background: var(--toolbar-bg);
            border-bottom: 1px solid var(--border-color);
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
            background: var(--btn-hover);
            border-color: var(--border-color);
        }
        .tool-active {
            background: var(--btn-active);
            border: 1px solid var(--border-color);
            border-bottom-color: var(--btn-active);
            font-weight: bold;
            color: var(--text-color);
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
            background: var(--panel-bg);
            border: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
            box-sizing: border-box;
        }
        .pane-right {
            width: 70%;
            background: var(--panel-bg);
            border: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
            box-sizing: border-box;
        }
        .pane-full {
            width: 100%;
            background: var(--panel-bg);
            border: 1px solid var(--border-color);
            padding: 10px;
            box-sizing: border-box;
            overflow-y: auto;
        }

        /* ビューのタイトルバー */
        .view-title {
            background: var(--header-bg);
            border-bottom: 1px solid var(--border-color);
            padding: 4px 8px;
            font-weight: bold;
            color: var(--header-text);
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
            color: var(--text-color);
        }
        .tree-link:hover {
            text-decoration: underline;
        }

        
        a {
            color: #0d47a1;
        }
        [data-theme="dark"] a {
            color: #64b5f6 !important;
        }

        /* テーブル */
        table {
            width: 100%;
            border-collapse: collapse;
            border: 1px solid var(--border-color);
            margin-bottom: 10px;
        }
        th, td {
            border: 1px solid var(--border-color);
            padding: 4px 6px;
            font-size: 12px;
            text-align: left;
        }
        th {
            background: var(--header-bg);
            color: var(--text-color);
            font-weight: normal;
            border-bottom: 2px solid #ccc;
        }
        tr:hover {
            background: #f2f7fc;
        }

        /* ボタン */
        button, .btn {
            background: linear-gradient(to bottom, #fcfcfc, #e6e6e6);
            border: 1px solid var(--border-color);
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
        [data-theme="dark"] button:hover, [data-theme="dark"] .btn:hover {
            background: linear-gradient(to bottom, #4a4a4a, #3d3d3d);
            border-color: #777;
        }
        .btn-danger {
            background: linear-gradient(to bottom, #fff5f5, #ffe0e0) !important;
            border-color: #cc9999 !important;
            color: #cc0000 !important;
        }
        [data-theme="dark"] .btn-danger {
            background: linear-gradient(to bottom, #4a1f1f, #331313) !important;
            border-color: #6e2e2e !important;
            color: #ff9999 !important;
        }
        .btn-danger:hover {
            background: #ffcccc !important;
        }
        [data-theme="dark"] .btn-danger:hover {
            background: #5a2f2f !important;
        }
        .btn-primary {
            background: linear-gradient(to bottom, #e3f2fd, #bbdefb) !important;
            border-color: #90caf9 !important;
            color: #0d47a1 !important;
            font-weight: bold;
        }
        [data-theme="dark"] .btn-primary {
            background: linear-gradient(to bottom, #1f3a4a, #132533) !important;
            border-color: #2e5a6e !important;
            color: #90caf9 !important;
            font-weight: bold;
        }
        .btn-primary:hover {
            background: #90caf9 !important;
        }
        [data-theme="dark"] .btn-primary:hover {
            background: #2e5a6e !important;
        }
        .btn-success {
            background: linear-gradient(to bottom, #f5fff5, #e0ffe0) !important;
            border-color: #99cc99 !important;
            color: #00aa00 !important;
        }
        [data-theme="dark"] .btn-success {
            background: linear-gradient(to bottom, #1f4a1f, #133313) !important;
            border-color: #2e6e2e !important;
            color: #99ff99 !important;
        }
        .btn-success:hover {
            background: #ccffcc !important;
        }
        [data-theme="dark"] .btn-success:hover {
            background: #2f5a2f !important;
        }

        /* 下部ステータス集計バー (Himenos再現) */
        .summary-bar {
            border-top: 1px solid var(--border-color);
            background: var(--status-bar-bg);
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
            color: var(--text-color);
            font-weight: bold;
            border-right: 1px solid #b0b0b0;
            text-decoration: none;
        }
        .summary-item:hover {
            opacity: 0.8;
        }
        .summary-red { background: #ff4d4d; color: #fff; }
        .summary-yellow { background: #ffeb3b; color: var(--text-color); }
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
            background: var(--status-bar-bg);
            border-top: 1px solid var(--border-color);
            color: var(--text-color);
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
        .status-warn { background: #ffeb3b; color: var(--text-color); }
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
        .flow-node-wait, .topo-node.flow-node-wait { background: #d3d3d3; color: #333; }
        .flow-node-run, .topo-node.flow-node-run { background: #2196f3; color: #fff; }
        .flow-node-success, .topo-node.flow-node-success { background: #4caf50; color: #fff; }
        .flow-node-warn, .topo-node.topo-warn, .topo-node.flow-node-warn { background: #ffeb3b; color: var(--text-color); }
        .flow-node-error, .topo-node.flow-node-error { background: #ff4d4d; color: #fff; }
        .flow-node-unknown, .topo-node.flow-node-unknown { background: #9e9e9e; color: #fff; }
        .flow-arrow { font-size: 16px; font-weight: bold; color: #666; }

        /* トポロジー監視マップ用スタイル */
        .topology-map {
            background: #ffffff;
            border: 1px solid var(--border-color);
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
            color: inherit;
        }
        .topo-node.topo-ok { background: #4caf50; color: #fff; }
        .topo-node.topo-err { background: #ff4d4d; color: #fff; }
        .topo-node.topo-warn { background: #ffeb3b; color: var(--text-color); }
        .topo-node.topo-unknown { background: #2196f3; color: #fff; }

        /* 3分割画面レイアウト用 */
        .split-layout {
            display: flex;
            flex-direction: column;
            gap: 5px;
            height: 100%;
            width: 100%;
            flex: 1;
            overflow: hidden;
        }
        .split-section {
            background: var(--panel-bg);
            border: 1px solid var(--border-color);
            display: flex;
            flex-direction: column;
            box-sizing: border-box;
            min-height: 150px;
        }
        
        /* モーダルダイアログのスタイル */
        .modal-overlay {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.4);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }
        .modal-card {
            background: #ffffff;
            border: 1px solid #d0d7de;
            border-radius: 6px;
            width: 400px;
            max-width: 90%;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            animation: modalFadeIn 0.2s ease-out;
        }
        .modal-header {
            background: linear-gradient(to bottom, #e1e9f6, #c5d7ed);
            border-bottom: 1px solid #99b4d1;
            padding: 8px 12px;
            font-weight: bold;
            color: #1e395b;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-top-left-radius: 5px;
            border-top-right-radius: 5px;
        }
        .modal-body {
            padding: 16px;
        }
        .modal-close-btn {
            background: transparent;
            border: none;
            font-size: 16px;
            cursor: pointer;
            font-weight: bold;
            color: #1e395b;
        }
        .modal-close-btn:hover {
            color: #ff0000;
        }
        @keyframes modalFadeIn {
            from { opacity: 0; transform: translateY(-20px); }
            to { opacity: 1; transform: translateY(0); }
        }
    
        /* ダークモード時の自動補完背景色対策 */
        [data-theme="dark"] input:-webkit-autofill,
        [data-theme="dark"] input:-webkit-autofill:hover, 
        [data-theme="dark"] input:-webkit-autofill:focus {
            -webkit-text-fill-color: #e0e0e0 !important;
            -webkit-box-shadow: 0 0 0px 1000px #2d2d2d inset !important;
            transition: background-color 5000s ease-in-out 0s;
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
        
        function toggleTargetField() {
            const typeSelect = document.querySelector('select[name="type"]');
            const targetInput = document.querySelector('input[name="target"]');
            if (!typeSelect || !targetInput) return;
            if (typeSelect.value === 'ping') {
                targetInput.disabled = true;
                targetInput.style.background = '#f0f0f0';
                targetInput.style.color = '#888';
                targetInput.style.cursor = 'not-allowed';
                targetInput.placeholder = 'PING時は自動でIPを使用します';
                targetInput.value = '';
            } else {
                targetInput.disabled = false;
                targetInput.style.background = '#ffffff';
                targetInput.style.color = '#000';
                targetInput.style.cursor = 'auto';
                targetInput.placeholder = '例: 80';
            }
        }
        window.addEventListener('DOMContentLoaded', toggleTargetField);
        
        function showAddMonitorModal() {
            const modal = document.getElementById('add_monitor_modal');
            if (modal) {
                modal.style.display = 'flex';
                toggleTargetField();
            }
        }
        function hideAddMonitorModal() {
            const modal = document.getElementById('add_monitor_modal');
            if (modal) {
                modal.style.display = 'none';
            }
        }
    </script>
<script>
        function toggleDarkMode() {
            const currentTheme = document.documentElement.getAttribute('data-theme');
            const targetTheme = currentTheme === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', targetTheme);
            localStorage.setItem('theme', targetTheme);
            updateThemeButtonLabel(targetTheme);
        }

        function updateThemeButtonLabel(theme) {
            const btn = document.getElementById('theme_btn');
            if (!btn) return;
            const isEn = localStorage.getItem('lang') === 'en';
            if (theme === 'dark') {
                btn.innerHTML = isEn ? '☀️ Light Mode' : '☀️ ライトモード';
            } else {
                btn.innerHTML = isEn ? '🌙 Dark Mode' : '🌙 ダークモード';
            }
        }

        // 初期ロード時のダークモード適用
        (function() {
            const savedTheme = localStorage.getItem('theme') || 'light';
            document.documentElement.setAttribute('data-theme', savedTheme);
            window.addEventListener('DOMContentLoaded', () => {
                updateThemeButtonLabel(savedTheme);
            });
        })();

        // 多言語翻訳
        const translations = {
            ja: {
                "jobs_manage": "📋 ジョブ管理",
                "nodes_manage": "🖥️ ノード・監視",
                "env_settings": "⚙️ 環境構築",
                "job_def_list": "ジョブ定義[一覧]",
                "real_script_list": "📁 実スクリプトファイル一覧",
                "job_def_detail": "ジョブ定義[詳細]",
                "job_history": "📊 ジョブ履歴",
                "session_detail": "セッション詳細",
                "schedule_list": "📅 スケジュール一覧",
                "schedule_add": "➕ スケジュール追加",
                "script_edit_title": "スクリプトファイルの直接編集",
                "script_path_lbl": "スクリプトパス:",
                "script_body_lbl": "スクリプト本文 (Git連携):",
                "btn_save": "💾 保存",
                "btn_cancel": "キャンセル",
                "settings_title": "環境構築 (通知設定 & バックアップ)",
                "settings_en_de": "💾 Himenos設定のインポート / エクスポート (YAML形式)",
                "settings_en_de_help": "すべての設定を一括バックアップ・復元できます。",
                "settings_export_btn": "📤 設定のエクスポート (himenos_backup.yaml ダウンロード)",
                "settings_import_lbl": "📥 設定のインポート (YAMLファイルをアップロード):",
                "settings_import_btn": "インポート実行",
                "settings_notify_title": "共通デフォルト通知設定",
                "settings_notify_lbl": "デフォルトの通知方法:",
                "settings_notify_opt_none": "通知しない",
                "settings_notify_opt_both": "メール & Slack 両方",
                "settings_notify_opt_slack": "Slack のみ",
                "settings_notify_opt_email": "メール のみ",
                "settings_notify_help": "※ 個別ジョブや監視で「デフォルト設定に従う」を選択した際、この設定が適用されます。",
                "settings_slack_title": "Slack 通知設定",
                "settings_smtp_title": "メール通知設定 (SMTP)",
                "settings_smtp_host": "SMTP サーバホスト名:",
                "settings_smtp_host_help": "※ 空欄の場合は、通知をシミュレートして標準ログへ出力します。",
                "settings_smtp_port": "SMTP ポート番号:",
                "settings_smtp_user": "SMTP ユーザ名:",
                "settings_smtp_pass": "SMTP パスワード:",
                "settings_smtp_from": "送信元アドレス (From):",
                "settings_smtp_to": "送信先アドレス (To):",
                "settings_save_btn": "💾 設定を保存",
                "settings_test_btn": "⚡ テスト送信",
                "sec_title": "🔒 セキュリティ設定",
                "sec_auth_title": "Basic認証 アカウント管理",
                "sec_ip_title": "接続許可IP制限管理",
                "sec_lbl_user": "ユーザー名:",
                "sec_lbl_pass": "パスワード:",
                "sec_btn_add": "➕ アカウント追加",
                "sec_ip_lbl": "接続許可IP / CIDR:",
                "sec_btn_ip_add": "➕ 制限追加",
                "sec_enable_ip": "IP制限を有効にする",
                "sec_btn_apply": "💾 設定適用",
                "sec_ip_rescue_help": "※ IP制限が有効な場合でも、ローカルホスト（127.0.0.1, ::1, localhost）からのアクセスは常に制限チェックから除外（救済許可）されます。これにより、誤ったIPアドレスを登録してしまい、管理者が自分自身でサーバーに二度とアクセスできなくなってしまう締め出し事故を完全に防ぎます。"
            },
            en: {
                "jobs_manage": "📋 Jobs",
                "nodes_manage": "🖥️ Nodes & Monitors",
                "env_settings": "⚙️ Settings",
                "job_def_list": "Job Definitions",
                "real_script_list": "📁 Real Scripts",
                "job_def_detail": "Job Details",
                "job_history": "📊 Job History",
                "session_detail": "Session Detail",
                "schedule_list": "📅 Schedules",
                "schedule_add": "➕ Add Schedule",
                "script_edit_title": "Direct Script Editor",
                "script_path_lbl": "Script Path:",
                "script_body_lbl": "Script Content (Git Linked):",
                "btn_save": "💾 Save",
                "btn_cancel": "Cancel",
                "settings_title": "Settings (Notifications & Backup)",
                "settings_en_de": "💾 Himenos Settings Import / Export (YAML)",
                "settings_en_de_help": "Backup and restore all settings at once.",
                "settings_export_btn": "📤 Export Settings (Download yaml)",
                "settings_import_lbl": "📥 Import Settings (Upload yaml file):",
                "settings_import_btn": "Execute Import",
                "settings_notify_title": "Global Default Notifications",
                "settings_notify_lbl": "Default Notify Method:",
                "settings_notify_opt_none": "Do not notify",
                "settings_notify_opt_both": "Both Email & Slack",
                "settings_notify_opt_slack": "Slack Only",
                "settings_notify_opt_email": "Email Only",
                "settings_notify_help": "* Applied when 'Default' is selected in individual jobs or monitors.",
                "settings_slack_title": "Slack Notifications",
                "settings_smtp_title": "Email Notifications (SMTP)",
                "settings_smtp_host": "SMTP Host Name:",
                "settings_smtp_host_help": "* If empty, SMTP notification is mocked and output to standard log.",
                "settings_smtp_port": "SMTP Port Number:",
                "settings_smtp_user": "SMTP User Name:",
                "settings_smtp_pass": "SMTP Password:",
                "settings_smtp_from": "Sender Address (From):",
                "settings_smtp_to": "Recipient Address (To):",
                "settings_save_btn": "💾 Save Settings",
                "settings_test_btn": "⚡ Test Send",
                "sec_title": "🔒 Security Settings",
                "sec_auth_title": "Basic Auth Accounts",
                "sec_ip_title": "Allowed IPs / CIDR Restriction",
                "sec_lbl_user": "Username:",
                "sec_lbl_pass": "Password:",
                "sec_btn_add": "➕ Add User",
                "sec_ip_lbl": "Allowed IP / CIDR:",
                "sec_btn_ip_add": "➕ Add IP Restriction",
                "sec_enable_ip": "Enable IP Restriction",
                "sec_btn_apply": "💾 Apply Settings",
                "sec_ip_rescue_help": "* Localhost (127.0.0.1, ::1, localhost) is always allowed even if IP restriction is enabled, preventing accidental lockout of the administrator."
            }
        };

        function toggleLanguage() {
            const currentLang = localStorage.getItem('lang') || 'ja';
            const targetLang = currentLang === 'ja' ? 'en' : 'ja';
            localStorage.setItem('lang', targetLang);
            applyLanguage(targetLang);
        }

                        function translateTextNodes(node, lang) {
            if (node.nodeType === 3) {
                let txt = node.nodeValue;
                if (!txt || txt.trim() === "") return;
                if (lang === 'en') {
                    txt = txt.replace(/正常終了/g, 'Success')
                             .replace(/異常終了/g, 'Abnormal')
                             .replace(/警告終了/g, 'Warning')
                             .replace(/実行中/g, 'Running')
                             .replace(/正常/g, 'Normal')
                             .replace(/異常/g, 'Abnormal')
                             .replace(/未判定/g, 'Pending')
                             .replace(/未実施/g, 'Not Run')
                             .replace(/不明/g, 'Unknown');
                } else {
                    txt = txt.replace(/Success/g, '正常終了')
                             .replace(/Abnormal/g, '異常終了')
                             .replace(/Warning/g, '警告終了')
                             .replace(/Running/g, '実行中')
                             .replace(/Normal/g, '正常')
                             .replace(/Pending/g, '未判定')
                             .replace(/Not Run/g, '未実施')
                             .replace(/Unknown/g, '不明');
                }
                node.nodeValue = txt;
            } else {
                if (node.tagName === 'SCRIPT' || node.tagName === 'STYLE' || node.tagName === 'TEXTAREA' || node.tagName === 'INPUT') return;
                node.childNodes.forEach(child => translateTextNodes(child, lang));
            }
        }

        function applyLanguage(lang) {
            document.querySelectorAll('[data-i18n]').forEach(el => {
                const key = el.getAttribute('data-i18n');
                if (translations[lang] && translations[lang][key]) {
                    el.innerHTML = translations[lang][key];
                }
            });
            
            translateTextNodes(document.body, lang);

            const langBtn = document.getElementById('lang_btn');
            if (langBtn) {
                langBtn.innerHTML = lang === 'ja' ? '🌐 English' : '🌐 日本語';
            }
            const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
            updateThemeButtonLabel(currentTheme);
        }

        // 初期ロード時の言語適用
        window.addEventListener('DOMContentLoaded', () => {
            const savedLang = localStorage.getItem('lang') || 'ja';
            applyLanguage(savedLang);
        });
    </script>
</head>
<body>

<div class="toolbar" style="justify-content: space-between;">
    <div style="display: flex; gap: 5px; align-items: center;">
        <a href="/?tab=jobs" class="tool-btn {{if or (eq .CurrentTab "jobs") (eq .CurrentTab "history") (eq .CurrentTab "schedules") (eq .CurrentTab "script_edit") (eq .CurrentTab "job_new") (eq .CurrentTab "job_edit")}}tool-active{{end}}"><span data-i18n="jobs_manage">📋 ジョブ管理</span></a>
        <a href="/?tab=nodes" class="tool-btn {{if or (eq .CurrentTab "nodes") (eq .CurrentTab "topology") (eq .CurrentTab "monitors") (eq .CurrentTab "nodes_manage") (eq .CurrentTab "node_new")}}tool-active{{end}}"><span data-i18n="nodes_manage">🖥️ ノード・監視</span></a>
        <a href="/?tab=settings" class="tool-btn {{if eq .CurrentTab "settings"}}tool-active{{end}}"><span data-i18n="env_settings">⚙️ 環境構築</span></a>
    </div>
    <div style="display: flex; gap: 5px; align-items: center; margin-left: auto; padding-right: 10px;">
        <button onclick="toggleLanguage()" class="btn" style="font-size: 11px; padding: 2px 8px; font-weight: bold; background: var(--panel-bg); color: var(--text-color); cursor: pointer;" id="lang_btn">🌐 English</button>
        <button onclick="toggleDarkMode()" class="btn" style="font-size: 11px; padding: 2px 8px; font-weight: bold; background: var(--panel-bg); color: var(--text-color); cursor: pointer;" id="theme_btn">🌓 Dark Mode</button>
    </div>
</div>

<div class="container">
    {{if or (eq .CurrentTab "jobs") (eq .CurrentTab "history") (eq .CurrentTab "schedules") (eq .CurrentTab "script_edit") (eq .CurrentTab "job_new") (eq .CurrentTab "job_edit") (eq .CurrentTab "jobs_manage")}}
        <!-- ジョブ管理 統合画面 -->
        <div class="split-layout">
            <!-- 1. ジョブ設定セクション -->
            <div class="split-section" style="flex: 1.3; min-height: 200px; display: flex; flex-direction: row; gap: 5px; box-sizing: border-box; overflow: hidden;">
                 <!-- 左ペイン：ジョブツリー -->
                 <div class="pane-left" style="flex: 1; border: none; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title">
                         <span data-i18n="job_def_list">ジョブ定義[一覧]</span>
                         <a href="/?tab=jobs{{if .SelectedJobID}}&s={{.SelectedJobID}}{{end}}" class="refresh-icon"><span data-i18n="btn_update">🔄 更新</span></a>
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
                         <div class="tree-node">📁 scripts</div>
                         {{range .ScriptFiles}}
                             <div class="tree-node">{{.Prefix}}📝 <a href="/?tab=script_edit&file={{.Path}}" class="tree-link">{{if .IsEnabled}}{{.Name}}{{else}}<span style="text-decoration: line-through; color: #888;">{{.Name}} (無効化中)</span>{{end}}</a></div>
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
                    <div class="error-message" style="background:#ffdddd; color:#cc0000; border:1px solid #ffbbbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">⚠️ {{.ErrorMessage}}</div>
                {{end}}
                {{if .SuccessMessage}}
                    <div class="success-message" style="background:#ddffdd; color:#00aa00; border:1px solid #bbffbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">✅ {{.SuccessMessage}}</div>
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
                    <div class="error-message" style="background:#ffdddd; color:#cc0000; border:1px solid #ffbbbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">⚠️ {{.ErrorMessage}}</div>
                {{end}}
                {{if .SuccessMessage}}
                    <div class="success-message" style="background:#ddffdd; color:#00aa00; border:1px solid #bbffbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">✅ {{.SuccessMessage}}</div>
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
                          <div class="view-title"><span><span data-i18n="script_edit_title">スクリプトファイルの直接編集</span>: {{.SelectedJobID}}</span></div>
                          <div class="view-content">
                              {{if .ErrorMessage}}
                    <div class="error-message" style="background:#ffdddd; color:#cc0000; border:1px solid #ffbbbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">⚠️ {{.ErrorMessage}}</div>
                {{end}}
                {{if .SuccessMessage}}
                    <div class="success-message" style="background:#ddffdd; color:#00aa00; border:1px solid #bbffbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">✅ {{.SuccessMessage}}</div>
                {{end}}
                              <form action="/action" method="POST" style="margin: 0;">
                                   <input type="hidden" name="action" value="save_script_only">
                                   <input type="hidden" name="filepath" value="{{.SelectedJobID}}">
                                   
                                   <div style="display: flex; gap: 8px; align-items: center; margin-bottom: 4px;">
                                       <span style="font-weight: bold; font-size: 11px; white-space: nowrap;" data-i18n="script_path_lbl">スクリプトパス:</span>
                                       <input type="text" value="{{.SelectedJobID}}" readonly style="background:#e0e0e0; cursor:not-allowed; flex: 1; font-size: 11px; padding: 2px;">
                                   </div>

                                   <label style="margin-top:2px; margin-bottom:2px; font-size:11px; font-weight: bold;" data-i18n="script_body_lbl">スクリプト本文 (Git連携):</label>
                                   <textarea name="script_content" rows="6" style="width:100%; height: 110px; font-family: monospace; font-size: 11px; box-sizing: border-box;">{{.ScriptContent}}</textarea>

                                   <div style="margin-top:4px; display: flex; gap: 5px; align-items: center;">
                                       <button type="submit" style="padding: 2px 8px; font-size: 11px;">💾 ファイル保存</button>
                                       <a href="/?tab=jobs" class="btn" style="padding: 2px 8px; font-size: 11px;">キャンセル</a>
                                   </div>
                               </form>

                               <div style="margin-top: 8px; border-top: 1px dashed #ccc; padding-top: 6px; display: flex; gap: 10px; align-items: flex-start;">
                                   <!-- 左：スクリプト状態切り替え -->
                                   <div style="flex: 1;">
                                       <form action="/action" method="POST" style="margin: 0;">
                                           <input type="hidden" name="action" value="toggle_script">
                                           <input type="hidden" name="file" value="{{.SelectedJobID}}">
                                           {{if .SelectedScriptEnabled}}
                                               <button type="submit" class="btn btn-danger" style="background: #dc3545; color: white; border-color: #dc3545; width: 100%; padding: 2px 8px; font-size: 11px;">🚫 スクリプト無効化</button>
                                           {{else}}
                                               <button type="submit" class="btn btn-success" style="background: #28a745; color: white; border-color: #28a745; width: 100%; padding: 2px 8px; font-size: 11px;">✅ スクリプト有効化</button>
                                           {{end}}
                                       </form>
                                   </div>

                                   <!-- 右：バインドされているジョブ -->
                                   <div style="flex: 1.5; padding: 4px 6px; background:#f9f9f9; border:1px solid #ccc; border-radius: 4px; box-sizing: border-box; min-height: 48px; font-size: 11px;">
                                       <span style="font-weight: bold;">🔗 バインド先ジョブ:</span>
                                       {{if .SelectedNode}}
                                           <strong>{{.SelectedNode.Name}}</strong>
                                           <div style="display: flex; gap: 3px; align-items: center; margin-top: 2px;">
                                               <form action="/action" method="POST" style="margin:0;">
                                                   <input type="hidden" name="action" value="run_job">
                                                   <input type="hidden" name="id" value="{{.SelectedNode.ID}}">
                                                   <button type="submit" style="font-size: 10px; padding: 1px 4px;">⚡ 実行</button>
                                               </form>
                                               <a href="/?tab=jobs&s={{.SelectedNode.ID}}" class="btn" style="font-size: 10px; padding: 1px 4px;">表示</a>
                                           </div>
                                       {{else}}
                                           <span style="color:#666;">なし</span>
                                       {{end}}
                                   </div>
                               </div>
                           </div>
                     {{else}}
                          <div class="view-title"><span data-i18n="job_def_detail">ジョブ定義[詳細]</span></div>
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
                                          {{if eq .SelectedNode.NotifyNormal "default"}}デフォルト設定に従う{{else if eq .SelectedNode.NotifyNormal "none"}}例外: 通知しない{{else if eq .SelectedNode.NotifyNormal "both"}}例外: メール & Slack 両方{{else if eq .SelectedNode.NotifyNormal "slack"}}例外: Slack のみ{{else if eq .SelectedNode.NotifyNormal "email"}}例外: メール のみ{{else}}デフォルト設定に従う{{end}}
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
            <div class="split-section" style="flex: 1; min-height: 150px; display: flex; flex-direction: row; gap: 5px; box-sizing: border-box; overflow: hidden;">
                 <div style="flex: 3; overflow-y: auto; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;">
                         <span data-i18n="job_history">📊 ジョブ履歴</span>
                         <a href="/?tab=jobs" class="refresh-icon"><span data-i18n="btn_update">🔄 更新</span></a>
                     </div>
                     <form action="/" method="GET" style="display: flex; gap: 5px; flex-wrap: wrap; margin-bottom: 10px; font-size:12px;">
                         <input type="hidden" name="tab" value="{{$.CurrentTab}}">
                         {{if $.SelectedJobID}}<input type="hidden" name="s" value="{{$.SelectedJobID}}">{{end}}
                         <input type="text" name="keyword" placeholder="キーワード検索" value="{{.HistoryKeyword}}" style="padding:2px; font-size:12px; width:120px;">
                         <input type="date" name="start_date" value="{{.HistoryStartDate}}" style="padding:2px; font-size:12px;">
                         <span>～</span>
                         <input type="date" name="end_date" value="{{.HistoryEndDate}}" style="padding:2px; font-size:12px;">
                         <button type="submit" style="padding:2px 8px; font-size:12px;">検索</button>
                         <a href="/?tab=jobs" style="padding:2px; font-size:12px;"><span data-i18n="btn_clear">クリア</span></a>
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
                                         {{else if eq .Status "異常終了"}}<span class="badge status-error">異常終了</span>
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
                         <div class="view-title" style="margin: -10px -10px 10px -10px;"><span><span data-i18n="session_detail">セッション詳細</span>: {{.SelectedSessionID}}</span></div>
                         <div style="font-size:12px; margin-bottom:10px;">
                             <strong>全体状態:</strong> 
                             {{if eq .SelectedSession.Status "正常終了"}}<span class="badge status-success">正常終了</span>
                             {{else if eq .SelectedSession.Status "異常終了"}}<span class="badge status-error">異常終了</span>
                             {{else if eq .SelectedSession.Status "警告終了"}}<span class="badge status-hold">警告終了</span>
                             {{else}}<span class="badge status-run">実行中</span>{{end}}
                             (正常: {{.GreenCount}} / 警告: {{.YellowCount}} / 異常: {{.RedCount}} / 合計: {{.TotalCount}})
                         </div>

                         <div class="topology-map" style="padding:10px; font-size:11px; margin-bottom:10px; max-height:120px; overflow-y:auto; white-space: normal;">{{range .SessionNodes}}<span class="topo-node {{if eq .Node.Status "実行中"}}flow-node-run{{else if eq .Node.Status "未実施"}}flow-node-unknown{{else if eq .Node.Status "起動失敗"}}flow-node-error{{else if eq .StatusLabel "正常終了"}}flow-node-success{{else if eq .StatusLabel "警告終了"}}flow-node-warn{{else if eq .StatusLabel "異常終了"}}flow-node-error{{else}}flow-node-success{{end}}"><a href="/?tab={{$.CurrentTab}}&session_id={{$.SelectedSessionID}}&show_log={{.Node.JobID}}{{if $.SelectedJobID}}&s={{$.SelectedJobID}}{{end}}" style="color:inherit;">{{.Node.Name}} [{{if eq .Node.Status "終了"}}{{.StatusLabel}}{{else}}{{.Node.Status}}{{end}}]</a></span><span class="flow-arrow"> → </span>{{end}}<span>完了</span></div>

                         {{if .ShowLogNode}}
                             <h5 style="margin:5px 0;">ログ: {{.ShowLogNode.Name}} (終了コード: {{.ShowLogNode.ExitValue}})</h5>
                             <textarea readonly rows="6" style="width:100%; font-family:monospace; font-size:11px; background:#fafafa;">{{.ShowLogNode.Log}}</textarea>
                         {{else}}
                             <p style="color:#666; font-size:11px;">フロー内のジョブ名をクリックすると実行ログが表示されます。</p>
                         {{end}}
                     {{else}}
                         <p style="color:#666; font-size:12px;">履歴一覧からセッションを選択すると詳細が表示されます。</p>
                     {{end}}
                 </div>
            </div>

            <!-- 3. スケジュール設定セクション -->
            <div class="split-section" style="flex: 1; min-height: 150px; display: flex; flex-direction: row; gap: 5px; box-sizing: border-box; overflow: hidden;">
                 <div style="flex: 1.5; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;">
                         <span data-i18n="schedule_list">📅 スケジュール一覧</span>
                         <a href="/?tab=jobs" class="refresh-icon"><span data-i18n="btn_update">🔄 更新</span></a>
                     </div>
                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px;">
                         <thead>
                             <tr>
                                 <th>スケジュール名</th>
                                 <th>実行対象ユニット/ジョブ</th>
                                 <th>設定</th>
                                 <th>状態</th>
                                 <th data-i18n="th_operation">操作</th>
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
                                         </td>
                                         {{else}}
                                             <span style="color: red; font-weight: bold;">⚠️ 存在しないジョブ: {{.JobID}}</span>
                                         </td>
                                         {{end}}
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
                                             <button type="submit" class="btn btn-danger" data-i18n="btn_delete">削除</button>
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
                     <div class="view-title" style="margin: -10px -10px 10px -10px;"><span data-i18n="schedule_add">➕ スケジュール追加</span></div>
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

                         <label style="margin-top:4px; margin-bottom:2px;">設定タイプ:</label>
                          <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 4px; margin-bottom: 4px;">
                              <label style="font-weight:normal; margin-top:0; font-size:11px; display: flex; align-items: center; gap: 2px;"><input type="radio" name="type" value="cron" checked onclick="updateScheduleForm()"> crontab</label>
                              <label style="font-weight:normal; margin-top:0; font-size:11px; display: flex; align-items: center; gap: 2px;"><input type="radio" name="type" value="daily" onclick="updateScheduleForm()"> 毎日</label>
                              <label style="font-weight:normal; margin-top:0; font-size:11px; display: flex; align-items: center; gap: 2px;"><input type="radio" name="type" value="weekly" onclick="updateScheduleForm()"> 毎週</label>
                              <label style="font-weight:normal; margin-top:0; font-size:11px; display: flex; align-items: center; gap: 2px;"><input type="radio" name="type" value="hourly" onclick="updateScheduleForm()"> 毎時</label>
                              <label style="font-weight:normal; margin-top:0; font-size:11px; display: flex; align-items: center; gap: 2px;"><input type="radio" name="type" value="interval" onclick="updateScheduleForm()"> 一定間隔</label>
                              <label style="font-weight:normal; margin-top:0; font-size:11px; display: flex; align-items: center; gap: 2px;"><input type="radio" name="type" value="datetime" onclick="updateScheduleForm()"> 日時指定</label>
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

                         <button type="submit" style="margin-top:8px; width:100%; padding:4px;"><span data-i18n="btn_register">➕ 登録</span></button>
                     </form>
                 </div>
            </div>
        </div>

    {{else if or (eq .CurrentTab "nodes") (eq .CurrentTab "topology") (eq .CurrentTab "monitors") (eq .CurrentTab "nodes_manage") (eq .CurrentTab "node_new")}}
        <!-- ノード・監視管理 統合画面 -->
        <div class="split-layout">
            <!-- 1. ノード一覧セクション -->
            <div class="split-section" style="flex: 1.2; min-height: 200px; display: flex; flex-direction: row; gap: 5px; box-sizing: border-box; overflow: hidden;">
                 <div style="flex: 1.5; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px;">
                         <span data-i18n="node_list_title">🖥️ ノード一覧</span>
                         <a href="/?tab=nodes" class="refresh-icon"><span data-i18n="btn_update">🔄 更新</span></a>
                     </div>
                     
                     <div style="margin-top: 5px; margin-bottom: 8px; display:flex; gap:3px; flex-wrap:wrap;">
                         <a href="/?tab={{$.CurrentTab}}&filter=all&sort={{$.SortKey}}&order={{$.SortOrder}}" class="btn {{if eq $.FilterStatus "all"}}btn-primary{{end}}" style="font-size:10px; padding:2px 6px;"><span data-i18n="filter_all">すべて</span></a>
                         <a href="/?tab={{$.CurrentTab}}&filter=abnormal&sort={{$.SortKey}}&order={{$.SortOrder}}" class="btn {{if eq $.FilterStatus "abnormal"}}btn-danger{{end}}" style="font-size:10px; padding:2px 6px; {{if ne $.FilterStatus "abnormal"}}background:#fff5f5; color:#cc0000; border-color:#cc9999;{{end}}"><span data-i18n="filter_abnormal">🔴 異常のみ</span></a>
                         <a href="/?tab={{$.CurrentTab}}&filter=normal&sort={{$.SortKey}}&order={{$.SortOrder}}" class="btn {{if eq $.FilterStatus "normal"}}btn-success{{end}}" style="font-size:10px; padding:2px 6px; {{if ne $.FilterStatus "normal"}}background:#f5fff5; color:#00aa00; border-color:#99cc99;{{end}}"><span data-i18n="filter_normal">🟢 正常のみ</span></a>
                         <a href="/?tab={{$.CurrentTab}}&filter=unknown&sort={{$.SortKey}}&order={{$.SortOrder}}" class="btn {{if eq $.FilterStatus "unknown"}}btn-primary{{end}}" style="font-size:10px; padding:2px 6px; {{if ne $.FilterStatus "unknown"}}background:#f5f5f5; color:#555; border-color:#ccc;{{end}}"><span data-i18n="filter_unknown">⚪ 未判定のみ</span></a>
                     </div>

                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px;">
                         <thead>
                             <tr>
                                 <th><a href="/?tab={{$.CurrentTab}}&filter={{$.FilterStatus}}&sort=name&order={{if eq $.SortKey "name"}}{{if eq $.SortOrder "asc"}}desc{{else}}asc{{end}}{{else}}asc{{end}}" style="color:inherit; text-decoration:none;">ノード名 {{if eq $.SortKey "name"}}{{if eq $.SortOrder "asc"}}▲{{else}}▼{{end}}{{end}}</a></th>
                                 <th><a href="/?tab={{$.CurrentTab}}&filter={{$.FilterStatus}}&sort=ip&order={{if eq $.SortKey "ip"}}{{if eq $.SortOrder "asc"}}desc{{else}}asc{{end}}{{else}}asc{{end}}" style="color:inherit; text-decoration:none;">IPアドレス {{if eq $.SortKey "ip"}}{{if eq $.SortOrder "asc"}}▲{{else}}▼{{end}}{{end}}</a></th>
                                 <th><a href="/?tab={{$.CurrentTab}}&filter={{$.FilterStatus}}&sort=status&order={{if eq $.SortKey "status"}}{{if eq $.SortOrder "asc"}}desc{{else}}asc{{end}}{{else}}asc{{end}}" style="color:inherit; text-decoration:none;">状態 (監視) {{if eq $.SortKey "status"}}{{if eq $.SortOrder "asc"}}▲{{else}}▼{{end}}{{end}}</a></th>
                             </tr>
                         </thead>
                         <tbody>
                             {{range .Nodes}}
                                 <tr class="{{if eq .ID $.SelectedNodeID}}tree-active{{end}}">
                                     <td><a href="/?tab={{$.CurrentTab}}&s={{.ID}}&filter={{$.FilterStatus}}&sort={{$.SortKey}}&order={{$.SortOrder}}">{{.Name}}</a></td>
                                     <td>{{.IPAddress}}</td>
                                     <td>
                                         {{if eq .Description "正常"}}<span class="badge status-success">正常</span>
                                         {{else if eq .Description "異常"}}<span class="badge status-error" style="background-color: #ff4d4d; color: #ffffff; font-weight: bold; border: 1px solid #d43f3a; padding: 2px 6px; box-shadow: 1px 1px 2px rgba(0,0,0,0.15);">異常</span>
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
                         <div style="margin-top:5px; display:flex; gap:5px;">
                             <button type="submit" class="btn btn-primary" style="font-size:12px; padding:3px 10px;">💾 ノード一括保存 (適用)</button>
                         </div>
                     </form>

                     <div style="margin-top: 15px; border-top: 1px dashed #ccc; padding-top: 10px; display: flex; gap: 5px;">
                         <form action="/action" method="POST" style="margin:0; flex:1;" onsubmit="return confirm('「異常」状態のノードをすべて削除しますか？');">
                             <input type="hidden" name="action" value="delete_nodes_by_status">
                             <input type="hidden" name="status" value="abnormal">
                             <button type="submit" class="btn btn-danger" style="width:100%; font-size:11px; padding:3px;"><span data-i18n="btn_del_abnormal">🗑️ 異常ノード一括削除</span></button>
                         </form>
                         <form action="/action" method="POST" style="margin:0; flex:1;" onsubmit="return confirm('「未実施/不明」状態のノードをすべて削除しますか？');">
                             <input type="hidden" name="action" value="delete_nodes_by_status">
                             <input type="hidden" name="status" value="unknown">
                             <button type="submit" class="btn" style="width:100%; font-size:11px; padding:3px; background: #e0e0e0; color: #333; border-color: #ccc;"><span data-i18n="btn_del_unknown">🗑️ 不明ノード一括削除</span></button>
                         </form>
                         <form action="/action" method="POST" style="margin:0; flex:1;" onsubmit="return confirm('すべてのノードを削除しますか？');">
                             <input type="hidden" name="action" value="delete_nodes_by_status">
                             <input type="hidden" name="status" value="all">
                             <button type="submit" class="btn btn-danger" style="width:100%; font-size:11px; padding:3px;"><span data-i18n="btn_del_all">🗑️ 全ノード削除</span></button>
                         </form>
                     </div>
                 </div>

                 <div style="flex: 1; padding: 10px; height: 100%; box-sizing: border-box; overflow-y: auto; font-size:12px;">
                     {{if .SelectedNodeData}}
                         <div class="view-title" style="margin: -10px -10px 10px -10px;"><span>ノード詳細: {{.SelectedNodeData.Name}}</span></div>
                         <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px; margin-bottom:10px;">
                             <tr><th data-i18n="th_ip_addr">IPアドレス</th><td>{{.SelectedNodeData.IPAddress}}</td></tr>
                             <tr><th>プラットフォーム</th><td>{{.SelectedNodeData.Platform}}</td></tr>
                             <tr><th>説明</th><td>{{.SelectedNodeData.Description}}</td></tr>
                         </table>

                         <h5 style="margin:10px 0 5px 0;">このノードの監視設定</h5>
                         <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:11px;">
                             <thead>
                                 <tr><th data-i18n="th_monitor_item">監視項目</th><th data-i18n="th_threshold">閾値</th><th>状態</th></tr>
                             </thead>
                             <tbody>
                                 {{range .SelectedNodeMonitors}}
                                     <tr>
                                         <td>{{.Type}} ({{.Target}})</td>
                                         <td>{{.Operator}} {{.ThresholdValue}}</td>
                                         <td>
                                             {{if eq .LastStatus "正常"}}<span class="badge status-success">正常</span>
                                             {{else if eq .LastStatus "異常"}}<span class="badge status-error">異常</span>
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
                             <a href="/?tab={{$.CurrentTab}}" class="btn"><span data-i18n="btn_clear">クリア</span></a>
                         </div>
                     {{else}}
                         <div class="view-title" style="margin: -10px -10px 10px -10px;"><span data-i18n="node_add_title">➕ 新規ノード登録</span></div>
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

                             <button type="submit" style="margin-top:10px; width:100%; padding:4px;"><span data-i18n="btn_register">➕ 登録</span></button>
                         </form>
                     {{end}}
                 </div>
            </div>

            <!-- 2. ノードマップセクション (グリッドレイアウトへ改善) -->
            <div class="split-section" style="min-height: 200px; padding:10px; height: 250px; overflow-y: auto; box-sizing: border-box;">
                 <div class="view-title" style="margin: -10px -10px 10px -10px;">
                     <span data-i18n="node_map_title">🗺️ ノードマップ (一覧トポロジー)</span>
                     <a href="/?tab=nodes" class="refresh-icon"><span data-i18n="btn_update">🔄 更新</span></a>
                 </div>
                 <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-top:10px; padding: 10px; background: #fafafa; border: 1px solid var(--border-color); max-height: 180px; overflow-y: auto;">
                     {{range .Nodes}}
                          <div class="topo-node {{if eq .Description "正常"}}topo-ok{{else if eq .Description "異常"}}topo-err{{else}}topo-unknown{{end}}" style="font-size: 11px; padding: 4px 8px; min-width: 140px; text-align: center; box-shadow: 1px 1px 3px rgba(0,0,0,0.1); border-radius: 4px; box-sizing: border-box;">
                              <a href="/?tab={{$.CurrentTab}}&s={{.ID}}&filter={{$.FilterStatus}}&sort={{$.SortKey}}&order={{$.SortOrder}}" style="color:inherit; text-decoration: none;">
                                  <strong>{{.Name}}</strong><br>
                                  <span style="font-size: 9px; opacity: 0.85;">{{.IPAddress}} [{{.Description}}]</span>
                              </a>
                          </div>
                     {{else}}
                          <div style="color: #666; font-size: 12px; padding: 10px;">登録ノードがありません。</div>
                     {{end}}
                 </div>
            </div>

            <!-- 3. 監視管理セクション -->
            <div class="split-section" style="flex: 1; min-height: 150px; display: flex; flex-direction: row; gap: 5px; box-sizing: border-box; overflow: hidden;">
                 <!-- A. 監視設定一覧 -->
                 <div style="flex: 1; padding: 10px; border-right: 1px solid #ccc; height: 100%; box-sizing: border-box; overflow-y: auto;">
                     <div class="view-title" style="margin: -10px -10px 10px -10px; display: flex; justify-content: space-between; align-items: center;">
                         <span data-i18n="node_monitor_title">🔍 監視設定一覧</span>
                         <div style="display:flex; gap:10px; align-items:center;">
                             <button onclick="showAddMonitorModal()" class="btn btn-primary" style="font-size:11px; padding:2px 8px; font-weight:bold; height:20px; line-height:14px; display:inline-flex; align-items:center;"><span data-i18n="btn_add_monitor">➕ 監視設定の追加</span></button>
                             <a href="/?tab=nodes" class="refresh-icon"><span data-i18n="btn_update">🔄 更新</span></a>
                         </div>
                     </div>
                     <div style="font-size:12px; margin-bottom:10px;">
                         <strong>全体ステータス:</strong> 
                         <span class="badge status-error">異常: {{.RedCount}}</span>
                         <span class="badge status-success">正常: {{.GreenCount}}</span>
                         <span class="badge status-unknown">不明: {{.BlueCount}}</span>
                         (合計: {{.TotalCount}} 件)
                     </div>

                     <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:12px; margin-bottom:15px;">
                         <thead>
                             <tr>
                                 <th data-i18n="th_target_node">対象ノード</th>
                                 <th data-i18n="th_monitor_item">監視項目</th>
                                 <th data-i18n="th_threshold">閾値</th>
                                 <th data-i18n="th_last_state">最終状態</th>
                                 <th data-i18n="th_operation">操作</th>
                             </tr>
                         </thead>
                         <tbody>
                              {{range .NodeMonitors}}
                                 <tr>
                                     <td>
                                         {{$nodeObj := index $.NodeMap .NodeID}}
                                         {{if $nodeObj}}{{$nodeObj.Name}}{{else}}{{.NodeID}}{{end}}
                                     </td>
                                     <td>{{.Type}} ({{.Target}})</td>
                                     <td>{{.Operator}} {{.ThresholdValue}}</td>
                                     <td>
                                         {{if eq .LastStatus "正常"}}<span class="badge status-success">正常</span>
                                         {{else if eq .LastStatus "異常"}}<span class="badge status-error">異常: {{.LastResultValue}}</span>
                                         {{else}}<span class="badge status-unknown">未判定</span>{{end}}
                                     </td>
                                     <td>
                                         <form action="/action" method="POST" style="display:inline;">
                                             <input type="hidden" name="action" value="delete_monitor">
                                             <input type="hidden" name="id" value="{{.ID}}">
                                             <button type="submit" class="btn btn-danger" data-i18n="btn_delete">削除</button>
                                         </form>
                                     </td>
                                 </tr>
                             {{else}}
                                 <tr><td colspan="5">監視設定が登録されていません。</td></tr>
                             {{end}}
                         </tbody>
                     </table>

                     </div>

                 <!-- B. 監視アラート履歴 -->
                 <div style="flex: 1; padding: 10px; height: 100%; box-sizing: border-box; overflow-y: auto;">
                      <div class="view-title" style="margin: -10px -10px 10px -10px;">
                          <span data-i18n="node_alert_title">📜 監視アラート履歴</span>
                      </div>
                      <table border="1" cellspacing="0" cellpadding="4" style="width:100%; font-size:11px; margin-top: 10px;">
                          <thead>
                              <tr>
                                  <th data-i18n="th_datetime">日時</th>
                                  <th data-i18n="th_target_node">対象ノード</th>
                                  <th data-i18n="th_item">項目</th>
                                  <th data-i18n="th_result">判定結果</th>
                                  <th data-i18n="th_log_value">ログ・結果値</th>
                              </tr>
                          </thead>
                          <tbody>
                              {{range .MonitorHistory}}
                                  <tr>
                                      <td>{{.CheckTime}}</td>
                                      <td>{{.NodeName}}</td>
                                      <td>{{.Name}} ({{.Type}})</td>
                                      <td>
                                          {{if eq .Status "正常"}}<span class="badge status-success">正常</span>
                                          {{else}}<span class="badge status-error">異常</span>{{end}}
                                      </td>
                                      <td>{{.Log}}</td>
                                  </tr>
                              {{else}}
                                  <tr><td colspan="5">監視履歴はありません。</td></tr>
                              {{end}}
                          </tbody>
                      </table>
                 </div>
                 </div>
            </div>
        </div>

    {{else if eq .CurrentTab "settings"}}
        <!-- 設定ペイン -->
        <div class="pane-full">
            <div class="view-title"><span data-i18n="settings_title">環境構築 (通知設定 & バックアップ)</span></div>
            <div class="view-content">
                {{if .ErrorMessage}}
                    <div class="error-message" style="background:#ffdddd; color:#cc0000; border:1px solid #ffbbbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">⚠️ {{.ErrorMessage}}</div>
                {{end}}
                {{if .SuccessMessage}}
                    <div class="success-message" style="background:#ddffdd; color:#00aa00; border:1px solid #bbffbb; padding:10px; border-radius:4px; margin-bottom:15px; font-weight:bold; font-size:12px;">✅ {{.SuccessMessage}}</div>
                {{end}}

                <div style="background: var(--alert-bg); padding:15px; border:1px solid var(--border-color); margin-bottom:20px; color: var(--text-color);">
                    <h3 data-i18n="settings_en_de">💾 Himenos設定のインポート / エクスポート (YAML形式)</h3>
                    <div class="help-text" data-i18n="settings_en_de_help">すべての設定を一括バックアップ・復元できます。</div>
                    <div style="margin-top:10px;">
                        <a href="/export" class="btn btn-primary"><span data-i18n="settings_export_btn">📤 設定のエクスポート (himenos_backup.yaml ダウンロード)</span></a>
                    </div>
                    <form action="/import" method="POST" enctype="multipart/form-data" style="margin-top:15px; border-top:1px solid var(--border-color); padding-top:10px;">
                        <label data-i18n="settings_import_lbl">📥 設定のインポート (YAMLファイルをアップロード):</label>
                        <input type="file" name="backup_file" required>
                        <button type="submit" style="margin-top:5px;" data-i18n="settings_import_btn">インポート実行</button>
                    </form>
                </div>

                <form action="/action" method="POST" style="margin: 0;">
                    <input type="hidden" name="action" id="settings_action_type" value="update_settings">

                    <!-- 1. 共通デフォルト設定カード -->
                    <div style="background: var(--card-bg); border: 1px solid var(--border-color); border-radius: 6px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); margin-bottom: 20px; color: var(--text-color);">
                        <h3 style="margin-top: 0; display: flex; align-items: center; gap: 5px; color: var(--text-color);"><span style="font-size: 20px;">🔔</span> <span data-i18n="settings_notify_title">共通デフォルト通知設定</span></h3>
                        <div style="display: flex; align-items: center; gap: 15px; margin-top: 10px;">
                            <label style="font-weight: bold; color: var(--text-color); margin-bottom: 0;" data-i18n="settings_notify_lbl">デフォルトの通知方法:</label>
                            <select name="default_notify" style="padding: 4px 8px; border: 1px solid #d0d7de; border-radius: 4px; font-size: 12px; background: #ffffff;">
                                <option value="" {{if eq .Settings.DefaultNotify ""}}selected{{end}} data-i18n="settings_notify_opt_none">通知しない</option>
                                <option value="both" {{if eq .Settings.DefaultNotify "both"}}selected{{end}} data-i18n="settings_notify_opt_both">メール & Slack 両方</option>
                                <option value="slack" {{if eq .Settings.DefaultNotify "slack"}}selected{{end}} data-i18n="settings_notify_opt_slack">Slack のみ</option>
                                <option value="email" {{if eq .Settings.DefaultNotify "email"}}selected{{end}} data-i18n="settings_notify_opt_email">メール のみ</option>
                            </select>
                        </div>
                        <div class="help-text" style="margin-top: 6px; color: var(--text-color); opacity: 0.8;"><span data-i18n="settings_notify_help">※ 個別ジョブや監視で「デフォルト設定に従う」を選択した際、この設定が適用されます。</span></div>
                    </div>

                    <!-- 2. 横並びの通知設定カードコンテナ -->
                    <div style="display: flex; gap: 20px; flex-wrap: wrap;">
                        
                        <!-- Slack設定カード -->
                        <div style="flex: 1; min-width: 320px; background: var(--card-bg); border: 1px solid var(--border-color); border-radius: 6px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); display: flex; flex-direction: column; justify-content: space-between; border-top: 4px solid #4a154b; color: var(--text-color);">
                            <div>
                                <h3 style="margin-top: 0; display: flex; align-items: center; gap: 6px; color: #8a358b;"><span style="font-size: 20px;">💬</span> <span data-i18n="settings_slack_title">Slack 通知設定</span></h3>
                                <div style="margin-top: 12px;">
                                    <label style="font-weight: bold; display: block; margin-bottom: 4px; color: var(--text-color);">Incoming Webhook URL:</label>
                                    <input type="text" name="slack_webhook" value="{{.Settings.SlackWebhook}}" placeholder="https://hooks.slack.com/services/..." style="width: 100%; padding: 6px; border: 1px solid #d0d7de; border-radius: 4px; box-sizing: border-box; font-size: 12px;">
                                </div>
                            </div>
                            <div style="margin-top: 25px; display: flex; gap: 8px;">
                                <button type="submit" onclick="document.getElementById('settings_action_type').value='update_settings';" class="btn btn-primary" style="flex: 1; padding: 8px; font-weight: bold; border-radius: 4px; font-size: 12px; cursor: pointer; text-align: center; height: 32px; display: inline-flex; justify-content: center; align-items: center;" data-i18n="settings_save_btn">💾 設定を保存</button>
                                <button type="submit" onclick="document.getElementById('settings_action_type').value='test_slack';" class="btn" style="flex: 1; background: #e8f0fe; color: #5495e8; border: 1px solid #d2e3fc; padding: 8px; font-weight: bold; border-radius: 4px; font-size: 12px; cursor: pointer; text-align: center; height: 32px; display: inline-flex; justify-content: center; align-items: center;">⚡ テスト送信</button>
                            </div>
                        </div>

                        <!-- SMTPメール設定カード -->
                        <div style="flex: 1.2; min-width: 380px; background: var(--card-bg); border: 1px solid var(--border-color); border-radius: 6px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); border-top: 4px solid #1a73e8; display: flex; flex-direction: column; justify-content: space-between; color: var(--text-color);">
                            <div>
                                <h3 style="margin-top: 0; display: flex; align-items: center; gap: 6px; color: #5495e8;"><span style="font-size: 20px;">✉️</span> <span data-i18n="settings_smtp_title">メール通知設定 (SMTP)</span></h3>
                                
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-top: 12px;">
                                    <div style="grid-column: span 2;">
                                        <label style="font-weight: bold; display: block; margin-bottom: 4px; color: var(--text-color);" data-i18n="settings_smtp_host">SMTP サーバホスト名:</label>
                                        <input type="text" name="smtp_host" value="{{.Settings.SMTPHost}}" placeholder="smtp.example.com" style="width: 100%; padding: 6px; border: 1px solid #d0d7de; border-radius: 4px; box-sizing: border-box; font-size: 12px;">
                                        <span style="font-size: 10px; color: var(--text-color); opacity: 0.7;"><span data-i18n="settings_smtp_host_help">※ 空欄の場合は、通知をシミュレートして標準ログへ出力します。</span>
                                    </div>
                                    <div>
                                        <label style="font-weight: bold; display: block; margin-bottom: 4px; color: var(--text-color);" data-i18n="settings_smtp_port">SMTP ポート番号:</label>
                                        <input type="text" name="smtp_port" value="{{.Settings.SMTPPort}}" placeholder="587" style="width: 100%; padding: 6px; border: 1px solid #d0d7de; border-radius: 4px; box-sizing: border-box; font-size: 12px;">
                                    </div>
                                    <div>
                                        <label style="font-weight: bold; display: block; margin-bottom: 4px; color: var(--text-color);" data-i18n="settings_smtp_user">SMTP ユーザ名:</label>
                                        <input type="text" name="smtp_user" value="{{.Settings.SMTPUser}}" style="width: 100%; padding: 6px; border: 1px solid #d0d7de; border-radius: 4px; box-sizing: border-box; font-size: 12px;">
                                    </div>
                                    <div style="grid-column: span 2;">
                                        <label style="font-weight: bold; display: block; margin-bottom: 4px; color: var(--text-color);" data-i18n="settings_smtp_pass">SMTP パスワード:</label>
                                        <input type="password" name="smtp_pass" value="{{.Settings.SMTPPass}}" style="width: 100%; padding: 6px; border: 1px solid #d0d7de; border-radius: 4px; box-sizing: border-box; font-size: 12px;">
                                    </div>
                                    <div>
                                        <label style="font-weight: bold; display: block; margin-bottom: 4px; color: var(--text-color);" data-i18n="settings_smtp_from">送信元アドレス (From):</label>
                                        <input type="text" name="smtp_from" value="{{.Settings.SMTPFrom}}" placeholder="sender@example.com" style="width: 100%; padding: 6px; border: 1px solid #d0d7de; border-radius: 4px; box-sizing: border-box; font-size: 12px;">
                                    </div>
                                    <div>
                                        <label style="font-weight: bold; display: block; margin-bottom: 4px; color: var(--text-color);" data-i18n="settings_smtp_to">送信先アドレス (To):</label>
                                        <input type="text" name="smtp_to" value="{{.Settings.SMTPTo}}" placeholder="receiver@example.com" style="width: 100%; padding: 6px; border: 1px solid #d0d7de; border-radius: 4px; box-sizing: border-box; font-size: 12px;">
                                    </div>
                                </div>
                            </div>
                            
                            <div style="margin-top: 20px; display: flex; gap: 8px;">
                                <button type="submit" onclick="document.getElementById('settings_action_type').value='update_settings';" class="btn btn-primary" style="flex: 1; padding: 8px; font-weight: bold; border-radius: 4px; font-size: 12px; cursor: pointer; text-align: center; height: 32px; display: inline-flex; justify-content: center; align-items: center;" data-i18n="settings_save_btn">💾 設定を保存</button>
                                <button type="submit" onclick="document.getElementById('settings_action_type').value='test_email';" class="btn" style="flex: 1; background: #e8f0fe; color: #5495e8; border: 1px solid #d2e3fc; padding: 8px; font-weight: bold; border-radius: 4px; font-size: 12px; cursor: pointer; text-align: center; height: 32px; display: inline-flex; justify-content: center; align-items: center;">⚡ テスト送信</button>
                            </div>
                        </div>

                    </div>
                
                    <!-- 3. セキュリティ設定（Basic認証 & IPアドレス制限） -->
                    <div style="background: var(--card-bg); border: 1px solid var(--border-color); border-radius: 6px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); margin-top: 20px; color: var(--text-color);">
                        <h3 style="margin-top: 0; display: flex; align-items: center; gap: 5px; color: var(--text-color);" data-i18n="sec_title">🔒 セキュリティ設定</h3>
                        
                        <div style="display: flex; gap: 20px; flex-wrap: wrap; margin-top: 15px;">
                            
                            <!-- Basic認証一覧 & 追加 -->
                            <div style="flex: 1; min-width: 300px; border-right: 1px dashed var(--border-color); padding-right: 15px;">
                                <h4 style="margin-top: 0;" data-i18n="sec_auth_title">Basic認証 アカウント管理</h4>
                                <table border="1" cellspacing="0" cellpadding="4" style="width: 100%; font-size: 11px; margin-bottom: 15px;">
                                    <thead>
                                        <tr>
                                            <th>ユーザー名</th>
                                            <th style="width: 60px;">操作</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {{range .Settings.BasicAuthUsers}}
                                            <tr>
                                                <td>{{.Username}}</td>
                                                <td>
                                                    <form action="/action" method="POST" style="margin: 0; display: inline;">
                                                        <input type="hidden" name="action" value="delete_basic_user">
                                                        <input type="hidden" name="username" value="{{.Username}}">
                                                        <button type="submit" class="btn btn-danger" style="font-size: 9px; padding: 1px 4px;">削除</button>
                                                    </form>
                                                </td>
                                            </tr>
                                        {{else}}
                                            <tr><td colspan="2" style="color: #888;">登録されたユーザーはいません。(デフォルト: admin/himenos)</td></tr>
                                        {{end}}
                                    </tbody>
                                </table>
                                
                                <form action="/action" method="POST" style="display: flex; flex-direction: column; gap: 8px; border-top: 1px solid var(--border-color); padding-top: 10px;">
                                    <input type="hidden" name="action" value="add_basic_user">
                                    <div style="display: flex; align-items: center; gap: 5px;">
                                        <label style="font-size: 11px; font-weight: bold; width: 80px;" data-i18n="sec_lbl_user">ユーザー名:</label>
                                        <input type="text" name="username" required style="flex: 1; font-size: 11px; padding: 2px;">
                                    </div>
                                    <div style="display: flex; align-items: center; gap: 5px;">
                                        <label style="font-size: 11px; font-weight: bold; width: 80px;" data-i18n="sec_lbl_pass">パスワード:</label>
                                        <input type="password" name="password" required style="flex: 1; font-size: 11px; padding: 2px;">
                                    </div>
                                    <button type="submit" class="btn btn-primary" style="font-size: 11px; padding: 4px;" data-i18n="sec_btn_add">➕ アカウント追加</button>
                                </form>
                            </div>
                            
                            <!-- IPアドレス制限一覧 & 追加 -->
                            <div style="flex: 1; min-width: 300px;">
                                <h4 style="margin-top: 0;" data-i18n="sec_ip_title">接続許可IP制限管理</h4>
                                <table border="1" cellspacing="0" cellpadding="4" style="width: 100%; font-size: 11px; margin-bottom: 15px;">
                                    <thead>
                                        <tr>
                                            <th>許可IP / CIDR</th>
                                            <th style="width: 60px;">操作</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {{range .Settings.AllowedIPs}}
                                            <tr>
                                                <td>{{.}}</td>
                                                <td>
                                                    <form action="/action" method="POST" style="margin: 0; display: inline;">
                                                        <input type="hidden" name="action" value="delete_allowed_ip">
                                                        <input type="hidden" name="ip" value="{{.}}">
                                                        <button type="submit" class="btn btn-danger" style="font-size: 9px; padding: 1px 4px;">削除</button>
                                                    </form>
                                                </td>
                                            </tr>
                                        {{else}}
                                            <tr><td colspan="2" style="color: #888;">IP制限が設定されていません。(全IPからの接続許可)</td></tr>
                                        {{end}}
                                    </tbody>
                                </table>
                                
                                <form action="/action" method="POST" style="display: flex; flex-direction: column; gap: 8px; border-top: 1px solid var(--border-color); padding-top: 10px; margin-bottom: 15px;">
                                    <input type="hidden" name="action" value="add_allowed_ip">
                                    <div style="display: flex; align-items: center; gap: 5px;">
                                        <label style="font-size: 11px; font-weight: bold; width: 110px;" data-i18n="sec_ip_lbl">接続許可IP / CIDR:</label>
                                        <input type="text" name="ip" placeholder="例: 192.168.1.50 または 192.168.1.0/24" required style="flex: 1; font-size: 11px; padding: 2px;">
                                    </div>
                                    <button type="submit" class="btn btn-primary" style="font-size: 11px; padding: 4px;" data-i18n="sec_btn_ip_add">➕ 制限追加</button>
                                </form>
                                
                                <form action="/action" method="POST" style="border-top: 1px solid var(--border-color); padding-top: 10px; display: flex; align-items: center; justify-content: space-between;">
                                    <input type="hidden" name="action" value="update_security_settings">
                                    <div style="display: flex; align-items: center; gap: 8px;">
                                        <input type="checkbox" name="ip_restriction_enabled" value="true" id="chk_ip_restrict" {{if .Settings.IPRestrictionEnabled}}checked{{end}}>
                                        <label for="chk_ip_restrict" style="font-size: 11px; font-weight: bold; margin-top: 0;" data-i18n="sec_enable_ip">IP制限を有効にする</label>
                                    </div>
                                    <div style="font-size: 10px; color: var(--text-color); opacity: 0.8; margin-top: 6px; line-height: 1.4; text-align: left;" data-i18n="sec_ip_rescue_help">
                                        ※ IP制限が有効な場合でも、ローカルホスト（127.0.0.1, ::1, localhost）からのアクセスは常に制限チェックから除外（救済許可）されます。これにより、誤ったIPアドレスを登録してしまい、管理者が自分自身でサーバーに二度とアクセスできなくなってしまう締め出し事故を完全に防ぎます。
                                    </div>
                                    <button type="submit" class="btn" style="font-size: 11px; padding: 3px 10px;" data-i18n="sec_btn_apply">💾 設定適用</button>
                                </form>
                            </div>
                            
                        </div>
                    </div>

                </form>
            </div>
        </div>
    {{end}}
</div>

<div class="status-bar">
    <span>接続先Himenosマネージャ(1/1)：ローカル(localhost)</span>
    <span>Himenos-Go v3.1 Web UI (w3m対応)</span>
</div>

<!-- 監視設定追加モーダルダイアログ -->
<div id="add_monitor_modal" class="modal-overlay" onclick="if(event.target===this) hideAddMonitorModal();">
    <div class="modal-card">
        <div class="modal-header">
            <span>➕ 監視設定追加</span>
            <button onclick="hideAddMonitorModal()" class="modal-close-btn">&times;</button>
        </div>
        <div class="modal-body" style="font-size:12px;">
            <form action="/action" method="POST" style="margin:0;">
                <input type="hidden" name="action" value="create_monitor">
                
                <label style="margin-top:0px; font-weight: bold; color: var(--text-color);">対象ノード:</label>
                <select name="node_id" style="width:100%; font-size:12px; padding:4px; border: 1px solid #d0d7de; border-radius: 4px; margin-bottom: 12px; background: #ffffff;">
                    {{range .Nodes}}
                        <option value="{{.ID}}">{{.Name}} ({{.IPAddress}})</option>
                    {{end}}
                </select>

                <label style="font-weight: bold; color: var(--text-color);">監視項目タイプ:</label>
                <select name="type" onchange="toggleTargetField()" style="width:100%; font-size:12px; padding:4px; border: 1px solid #d0d7de; border-radius: 4px; margin-bottom: 12px; background: #ffffff;">
                    <option value="ping">PING 応答監視 (死活監視)</option>
                    <option value="port">TCP ポート接続確認</option>
                </select>

                <label style="font-weight: bold; color: var(--text-color);">ターゲット (ポート番号など):</label>
                <input type="text" name="target" placeholder="例: 80" style="width:100%; font-size:12px; padding:4px; border: 1px solid #d0d7de; border-radius: 4px; margin-bottom: 15px; box-sizing: border-box;">

                <button type="submit" class="btn btn-primary" style="width:100%; padding:6px; font-weight: bold; border-radius: 4px; cursor: pointer; text-align: center;"><span data-i18n="btn_register">➕ 登録</span></button>
            </form>
        </div>
    </div>
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
	HistoryStartDate      string
	HistoryEndDate        string
	RedCount              int
	YellowCount           int
	GreenCount            int
	BlueCount             int
	TotalCount            int
	ScriptFiles           []ScriptFileInfo
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
	SuccessMessage    string
	Nodes             []*ManagedNode
	SelectedNodeID    string
	SelectedNodeData  *ManagedNode
	NodeMonitors      []*MonitorSetting
	SelectedNodeMonitors []*MonitorSetting
	NodeMap           map[string]*ManagedNode
	MonitorHistory    []MonitorHistory
	SortKey           string
	SortOrder         string
	FilterStatus      string
}

type SessionNodeItem struct {
	Node        *NodeState
	StatusLabel string
}

func ipToUint32(ipStr string) uint32 {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return 0
	}
	ipv4 := ip.To4()
	if ipv4 == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ipv4)
}



func parseIP(remoteAddr string) string {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return ip
}

func checkIP(clientIP string, allowedIPs []string) bool {
	if len(allowedIPs) == 0 {
		return true
	}
	parsedClientIP := net.ParseIP(clientIP)
	if parsedClientIP == nil {
		return false
	}
	for _, allowed := range allowedIPs {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		if strings.Contains(allowed, "/") {
			_, subnet, err := net.ParseCIDR(allowed)
			if err == nil && subnet.Contains(parsedClientIP) {
				return true
			}
		} else {
			if allowed == clientIP {
				return true
			}
		}
	}
	return false
}

func basicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		engine.mu.Lock()
		settings := engine.settings
		engine.mu.Unlock()

		// 1. IPアドレス制限
		if settings.IPRestrictionEnabled && len(settings.AllowedIPs) > 0 {
			clientIP := parseIP(r.RemoteAddr)
			if clientIP != "127.0.0.1" && clientIP != "::1" && clientIP != "localhost" {
				if !checkIP(clientIP, settings.AllowedIPs) {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte("Forbidden. Your IP (" + clientIP + ") is not allowed.\n"))
					return
				}
			}
		}

		// 2. Basic認証
		username, password, ok := r.BasicAuth()
		users := settings.BasicAuthUsers
		if len(users) == 0 {
			users = []BasicAuthUser{{Username: "admin", Password: "himenos"}}
		}

		authenticated := false
		if ok {
			for _, user := range users {
				if username == user.Username && password == user.Password {
					authenticated = true
					break
				}
			}
		}

		if !authenticated {
			w.Header().Set("WWW-Authenticate", `Basic realm="Himenos System Login"`)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("Unauthorized. Access Denied.\n"))
			return
		}
		next(w, r)
	}
}


func main() {

	initEngine()
	StartScheduler()
	engine.StartMonitoring()

	tmpl := template.Must(template.New("index").Parse(htmlTemplate))

	http.HandleFunc("/", basicAuth(func(w http.ResponseWriter, r *http.Request) {
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
			NodeMap:     engine.nodes,
			ErrorMessage: r.URL.Query().Get("err"),
			SuccessMessage: r.URL.Query().Get("msg"),
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
			data.HistoryStartDate = startDateStr
			data.HistoryEndDate = endDateStr

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
			id := r.URL.Query().Get("s")
			sortKey := r.URL.Query().Get("sort")
			if sortKey == "" {
				sortKey = "ip"
			}
			sortOrder := r.URL.Query().Get("order")
			if sortOrder == "" {
				sortOrder = "asc"
			}
			filterStatus := r.URL.Query().Get("filter")
			if filterStatus == "" {
				filterStatus = "all"
			}

			data.SortKey = sortKey
			data.SortOrder = sortOrder
			data.FilterStatus = filterStatus

			// 1. ノードマップ(トポロジー)＆監視最悪ステータス集計
			var allNodes []*ManagedNode
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
				allNodes = append(allNodes, n)
			}

			// CSVテキストの生成 (フィルター前の状態を常に全件保持)
			var lines []string
			for _, n := range allNodes {
				lines = append(lines, fmt.Sprintf("%s,%s,%s,%s", n.Name, n.IPAddress, n.Platform, n.Description))
			}
			data.NodeCsvText = strings.Join(lines, "\n")

			// 2. フィルタリングの適用
			var filteredNodes []*ManagedNode
			for _, n := range allNodes {
				if filterStatus == "abnormal" && n.Description != "異常" {
					continue
				}
				if filterStatus == "normal" && n.Description != "正常" {
					continue
				}
				if filterStatus == "unknown" && n.Description != "未実施" {
					continue
				}
				filteredNodes = append(filteredNodes, n)
			}

			// 3. ソートの適用
			sort.Slice(filteredNodes, func(i, j int) bool {
				valI, valJ := filteredNodes[i], filteredNodes[j]
				isLess := false

				switch sortKey {
				case "name":
					isLess = valI.Name < valJ.Name
				case "status":
					prio := func(status string) int {
						switch status {
						case "異常":
							return 3
						case "未実施":
							return 2
						case "正常":
							return 1
						default:
							return 0
						}
					}
					isLess = prio(valI.Description) > prio(valJ.Description)
				case "ip":
					fallthrough
				default:
					isLess = ipToUint32(valI.IPAddress) < ipToUint32(valJ.IPAddress)
				}

				if sortOrder == "desc" {
					return !isLess
				}
				return isLess
			})

			data.Nodes = filteredNodes

			if id != "" {
				if n, exists := engine.nodes[id]; exists {
					data.SelectedNodeID = id
					data.SelectedNodeData = n
					for _, m := range engine.monitors {
						if m.NodeID == id {
							data.SelectedNodeMonitors = append(data.SelectedNodeMonitors, m)
						}
					}
				}
			}

			// 4. 監視管理データロード (全体)
			data.MonitorHistory = engine.monHistory
			var allMonitors []*MonitorSetting
			for _, m := range engine.monitors {
				allMonitors = append(allMonitors, m)
			}
			sort.Slice(allMonitors, func(i, j int) bool {
				valI, valJ := allMonitors[i], allMonitors[j]
				nodeI := engine.nodes[valI.NodeID]
				nodeJ := engine.nodes[valJ.NodeID]
				if nodeI == nil && nodeJ == nil {
					return valI.ID < valJ.ID
				}
				if nodeI == nil {
					return true
				}
				if nodeJ == nil {
					return false
				}
				isLess := false
				switch sortKey {
				case "name":
					isLess = nodeI.Name < nodeJ.Name
				case "status":
					prio := func(status string) int {
						switch status {
						case "異常":
							return 3
						case "未実施":
							return 2
						case "正常":
							return 1
						default:
							return 0
						}
					}
					isLess = prio(valI.LastStatus) > prio(valJ.LastStatus)
				case "ip":
					fallthrough
				default:
					isLess = ipToUint32(nodeI.IPAddress) < ipToUint32(nodeJ.IPAddress)
				}
				if sortOrder == "desc" {
					return !isLess
				}
				return isLess
			})
			data.NodeMonitors = allMonitors
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
	}))

	http.HandleFunc("/export", basicAuth(func(w http.ResponseWriter, r *http.Request) {
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
	}))

	http.HandleFunc("/import", basicAuth(func(w http.ResponseWriter, r *http.Request) {
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
	}))

	http.HandleFunc("/action", basicAuth(func(w http.ResponseWriter, r *http.Request) {
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

			            // 全登録ノードに対してデフォルトのPING死活監視が存在しない場合は自動補完
            for nodeID, node := range newNodes {
                hasPing := false
                for _, m := range engine.monitors {
                    if m.NodeID == nodeID && m.Type == "ping" {
                        hasPing = true
                        break
                    }
                }
                if !hasPing {
                    monitorID := "mon_ping_" + nodeID
                    engine.monitors[monitorID] = &MonitorSetting{
                        ID:         monitorID,
                        Name:       "Ping監視",
                        NodeID:     nodeID,
                        Type:       "ping",
                        Target:     node.IPAddress,
                        Enabled:    true,
                        LastStatus: "未実施",
                    }
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

		case "delete_nodes_by_status":
			status := r.FormValue("status")
			engine.mu.Lock()
			var toDelete []string
			for id, n := range engine.nodes {
				if status == "all" {
					toDelete = append(toDelete, id)
				} else if status == "abnormal" && n.Description == "異常" {
					toDelete = append(toDelete, id)
				} else if status == "unknown" && n.Description != "正常" && n.Description != "異常" {
					toDelete = append(toDelete, id)
				}
			}
			for _, id := range toDelete {
				delete(engine.nodes, id)
				for mID, m := range engine.monitors {
					if m.NodeID == id {
						delete(engine.monitors, mID)
					}
				}
			}
			engine.saveNodes()
			engine.saveMonitors()
			engine.mu.Unlock()
			redirectTo = "/?tab=nodes"

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

		
		case "add_basic_user":
			username := r.FormValue("username")
			password := r.FormValue("password")
			engine.mu.Lock()
			if username != "" && password != "" {
				exists := false
				for _, u := range engine.settings.BasicAuthUsers {
					if u.Username == username {
						exists = true
						break
					}
				}
				if !exists {
					engine.settings.BasicAuthUsers = append(engine.settings.BasicAuthUsers, BasicAuthUser{Username: username, Password: password})
					engine.saveSettings()
				}
			}
			engine.mu.Unlock()
			redirectTo = "/?tab=settings"

		case "delete_basic_user":
			username := r.FormValue("username")
			engine.mu.Lock()
			var newUsers []BasicAuthUser
			for _, u := range engine.settings.BasicAuthUsers {
				if u.Username != username {
					newUsers = append(newUsers, u)
				}
			}
			engine.settings.BasicAuthUsers = newUsers
			engine.saveSettings()
			engine.mu.Unlock()
			redirectTo = "/?tab=settings"

		case "add_allowed_ip":
			ip := r.FormValue("ip")
			engine.mu.Lock()
			if ip != "" {
				exists := false
				for _, allowed := range engine.settings.AllowedIPs {
					if allowed == ip {
						exists = true
						break
					}
				}
				if !exists {
					engine.settings.AllowedIPs = append(engine.settings.AllowedIPs, ip)
					engine.saveSettings()
				}
			}
			engine.mu.Unlock()
			redirectTo = "/?tab=settings"

		case "delete_allowed_ip":
			ip := r.FormValue("ip")
			engine.mu.Lock()
			var newIPs []string
			for _, allowed := range engine.settings.AllowedIPs {
				if allowed != ip {
					newIPs = append(newIPs, allowed)
				}
			}
			engine.settings.AllowedIPs = newIPs
			engine.saveSettings()
			engine.mu.Unlock()
			redirectTo = "/?tab=settings"

		case "update_security_settings":
			ipRestrict := r.FormValue("ip_restriction_enabled") == "true"
			engine.mu.Lock()
			engine.settings.IPRestrictionEnabled = ipRestrict
			engine.saveSettings()
			engine.mu.Unlock()
			redirectTo = "/?tab=settings"

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

		case "test_slack":
			webhookURL := r.FormValue("slack_webhook")
			if webhookURL == "" {
				webhookURL = engine.settings.SlackWebhook
			}
			if webhookURL == "" {
				redirectTo = "/?tab=settings&err=" + url.QueryEscape("Slack Webhook URLが設定されていません。")
			} else {
				sendSlack(webhookURL, "これは Himenos からの Slack テスト通知です。正常に送信されました！")
				redirectTo = "/?tab=settings&msg=" + url.QueryEscape("Slack通知のテスト送信を実行しました。Slackチャンネルを確認してください。")
			}

		case "test_email":
			settings := Settings{
				SMTPHost: r.FormValue("smtp_host"),
				SMTPPort: r.FormValue("smtp_port"),
				SMTPUser: r.FormValue("smtp_user"),
				SMTPPass: r.FormValue("smtp_pass"),
				SMTPFrom: r.FormValue("smtp_from"),
				SMTPTo:   r.FormValue("smtp_to"),
			}
			if settings.SMTPHost == "" { settings.SMTPHost = engine.settings.SMTPHost }
			if settings.SMTPPort == "" { settings.SMTPPort = engine.settings.SMTPPort }
			if settings.SMTPUser == "" { settings.SMTPUser = engine.settings.SMTPUser }
			if settings.SMTPPass == "" { settings.SMTPPass = engine.settings.SMTPPass }
			if settings.SMTPFrom == "" { settings.SMTPFrom = engine.settings.SMTPFrom }
			if settings.SMTPTo == "" { settings.SMTPTo = engine.settings.SMTPTo }

			if settings.SMTPHost == "" || settings.SMTPTo == "" {
				if settings.SMTPTo == "" {
					redirectTo = "/?tab=settings&err=" + url.QueryEscape("送信先メールアドレス(To)が入力されていません。")
				} else {
					sendEmail(settings, "Himenos Email Test", "これは Himenos からのメールテスト通知です。SMTPHostが未設定のため、ログへ出力されました。")
					redirectTo = "/?tab=settings&msg=" + url.QueryEscape("SMTP未設定のため、通知ログにテストメールを出力しました（モック動作）。")
				}
			} else {
				sendEmail(settings, "Himenos Email Test", "これは Himenos からのメールテスト通知です。正常に送信されました！")
				redirectTo = "/?tab=settings&msg=" + url.QueryEscape("SMTPメールのテスト送信を実行しました。受信ボックスを確認してください。")
			}
		}

		http.Redirect(w, r, redirectTo, http.StatusSeeOther)
	}))

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
