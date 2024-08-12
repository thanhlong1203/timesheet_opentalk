package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

type VoiceChannelUser struct {
	ID          int64  `json:"id"`
	UserID      string `json:"user_id"`
	ClanID      int64  `json:"clan_id"`
	ChannelID   int64  `json:"channel_id"`
	DisplayName string `json:"display_name"`
	CreateTime  string `json:"create_time"`
	UpdateTime  string `json:"update_time"`
	Active      int16  `json:"active"`
}

type Session struct {
	Name      string    `json:"fullName"`
	Email     string    `json:"email"`
	GoogleID  string    `json:"google_id"`
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
}

type SessionTime struct {
	Name      string        `json:"fullName"`
	Email     string        `json:"email"`
	GoogleID  string        `json:"googleId"`
	TotalTime time.Duration `json:"totalTime"`
}

// Custom JSON marshaling
func (s SessionTime) MarshalJSON() ([]byte, error) {
	type Alias SessionTime
	return json.Marshal(&struct {
		TotalTime string `json:"totalTime"`
		*Alias
	}{
		// Convert `TotalTime` to string in the format "hh:mm:ss"
		TotalTime: fmt.Sprintf("%02d:%02d:%02d", int64(s.TotalTime.Hours()), int64(s.TotalTime.Minutes())%60, int64(s.TotalTime.Seconds())%60),
		Alias:     (*Alias)(&s),
	})
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	// Get environment variables from .env file
	user := os.Getenv("DB_USERNAME")
	password := os.Getenv("DB_PASSWORD")
	dbname := os.Getenv("DB_DATABASE")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	sslmode := os.Getenv("DB_SSLMODE")
	api := os.Getenv("API_SERVER")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s", user, password, dbname, host, port, sslmode)

	// Get the current time and format it to a string according to RFC3339
	now := time.Now()
	utcNow := now.UTC()
	twoDaysAgo := utcNow.AddDate(0, 0, -6)           // Lấy ngày 06/08/2024
	formattedTime := twoDaysAgo.Format(time.RFC3339) // Định dạng thời gian thành chuỗi theo RFC3339
	date := mustParseTime(formattedTime)
	fmt.Println("Time: ", formattedTime)

	activities, err := FetchActivities(connStr, date)
	if err != nil {
		log.Fatal(err)
	}

	// Sort by name and creation time
	SortActivities(activities)

	// In ra danh sách hoạt động
	fmt.Println("------------------------------------------------------------------------------------------------")
	fmt.Println("Activities")
	for _, activity := range activities {
		fmt.Printf("ID: %d, UserId: %s, ClanID: %d, ChannelID: %d, DisplayName: %s, CreateTime: %s, UpdateTime: %s, Active: %d\n",
			activity.ID, activity.UserID, activity.ClanID, activity.ChannelID, activity.DisplayName, activity.CreateTime, activity.UpdateTime, activity.Active)
	}

	// Handle user sessions
	sessions := processActivities(activities)

	// In ra danh sách phiên hoạt động
	fmt.Println("------------------------------------------------------------------------------------------------")
	fmt.Println("Sessions")
	for _, session := range sessions {
		fmt.Printf("DisplayName: %s, Email: %s, GoogleID: %s ,StartTime: %s,EndTime: %s\n",
			session.Name, session.Email, session.GoogleID, session.StartTime.Format(time.RFC3339), session.EndTime.Format(time.RFC3339))
	}

	// Filters sessions that reside entirely within other sessions
	filteredSessions := FilterSessions(sessions)

	fmt.Println("------------------------------------------------------------------------------------------------")
	fmt.Println("filteredSessions")
	for _, session := range filteredSessions {
		fmt.Printf("DisplayName: %s, Email: %s, GoogleID: %s ,StartTime: %s,EndTime: %s\n",
			session.Name, session.Email, session.GoogleID, session.StartTime.Format(time.RFC3339), session.EndTime.Format(time.RFC3339))
	}

	// Calculate the total time of each opentalk participant during the day
	totalTime := CalculateTotalTimeForDate(filteredSessions, date)

	fmt.Println("------------------------------------------------------------------------------------------------")
	fmt.Println("TotalTime")
	for _, sessionTime := range totalTime {
		fmt.Printf("Name: %s, Email: %s, GoogleID: %s, TotalTime: %v\n",
			sessionTime.Name, sessionTime.Email, sessionTime.GoogleID, sessionTime.TotalTime)
	}

	// Send data to API server
	SendRequest(api, totalTime)
}

// Get data from database
func FetchActivities(connStr string, date time.Time) ([]VoiceChannelUser, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Calculate the start and end of the day in UTC
	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour).Add(-time.Second)

	// Print start and end of day for debugging
	fmt.Println("startOfDay: ", startOfDay)
	fmt.Println("endOfDay: ", endOfDay)

	startOfDayStr := startOfDay.Format(time.RFC3339)
	endOfDayStr := endOfDay.Format(time.RFC3339)

	tableName := os.Getenv("VOICE_CHANNEL_USER_TABLE")
	if tableName == "" {
		return nil, fmt.Errorf("table name not set in environment")
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE create_time BETWEEN $1 AND $2", tableName)
	rows, err := db.Query(query, startOfDayStr, endOfDayStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var activities []VoiceChannelUser
	for rows.Next() {
		var ua VoiceChannelUser
		err := rows.Scan(&ua.ID, &ua.UserID, &ua.ClanID, &ua.ChannelID, &ua.DisplayName, &ua.CreateTime, &ua.UpdateTime, &ua.Active)
		if err != nil {
			return nil, err
		}
		activities = append(activities, ua)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return activities, nil
}

// Sort by name and creation time
func SortActivities(activities []VoiceChannelUser) {
	const timeLayout = time.RFC3339

	sort.Slice(activities, func(i, j int) bool {
		timeI, errI := time.Parse(timeLayout, activities[i].CreateTime)
		if errI != nil {
			timeI = time.Time{}
		}
		timeJ, errJ := time.Parse(timeLayout, activities[j].CreateTime)
		if errJ != nil {
			timeJ = time.Time{}
		}

		if activities[i].DisplayName == activities[j].DisplayName {
			return timeI.Before(timeJ)
		}
		return activities[i].DisplayName < activities[j].DisplayName
	})
}

// Handle sessions and provide start and end times for each session
func processActivities(activities []VoiceChannelUser) []Session {
	userSessions := make(map[string][]VoiceChannelUser)
	for _, activity := range activities {
		userSessions[activity.DisplayName] = append(userSessions[activity.DisplayName], activity)
	}

	var sessions []Session
	const timeLayout = time.RFC3339
	for _, userActivities := range userSessions {
		var currentSession *Session
		for _, activity := range userActivities {
			if activity.Active == 2 {
				if currentSession == nil {
					startTime, err := time.Parse(timeLayout, activity.CreateTime)
					if err != nil {
						return nil
					}

					endTime, err := time.Parse(timeLayout, activity.UpdateTime)
					if err != nil {
						return nil
					}
					currentSession = &Session{
						Name:      activity.DisplayName,
						Email:     "",
						GoogleID:  activity.UserID,
						StartTime: startTime,
						EndTime:   endTime,
					}
				} else {
					startTime, err := time.Parse(timeLayout, activity.CreateTime)
					if err != nil {
						return nil
					}

					endTime, err := time.Parse(timeLayout, activity.UpdateTime)
					if err != nil {
						return nil
					}
					currentSession.StartTime = minTime(currentSession.StartTime, startTime)
					currentSession.EndTime = maxTime(currentSession.EndTime, endTime)
				}

			} else if activity.Active == 0 && currentSession != nil {
				endTime, err := time.Parse(timeLayout, activity.UpdateTime)
				if err != nil {
					return nil
				}
				currentSession.EndTime = maxTime(currentSession.EndTime, endTime)
				sessions = append(sessions, *currentSession)
				currentSession = nil
			}
		}
		if currentSession != nil {
			sessions = append(sessions, *currentSession)
		}
	}

	return sessions
}

// Get min time
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// Get max time
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// Handles sessions that reside entirely within another session
func FilterSessions(sessions []Session) []Session {
	// Sort sessions by Name and StartTime
	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].Name == sessions[j].Name {
			return sessions[i].StartTime.Before(sessions[j].StartTime)
		}
		return sessions[i].Name < sessions[j].Name
	})

	var filtered []Session

	for i := 0; i < len(sessions); i++ {
		current := sessions[i]
		isSubSession := false

		// Checks if the current session is contained in any previous sessions with the same name
		for j := 0; j < i; j++ {
			if sessions[j].Name == current.Name &&
				sessions[j].StartTime.Before(current.StartTime) &&
				sessions[j].EndTime.After(current.EndTime) {
				isSubSession = true
				break
			}
		}

		// If it is not a secondary session, add it to the filtered list
		if !isSubSession {
			filtered = append(filtered, current)
		}
	}

	return filtered
}

// Format time to RFC3339
func mustParseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}

// Calculate the total activity time of each user within a certain level in 1 day
func CalculateTotalTimeForDate(sessions []Session, date time.Time) map[string]SessionTime {
	totalTimeMap := make(map[string]SessionTime)
	fmt.Println("Time2: ", date)

	// Determine the time interval from 3 to 5 UTC (10 to 12 UTC +7)
	startOfDay := date.Truncate(24 * time.Hour)
	fmt.Println("startOfDay: ", startOfDay)
	start3h := startOfDay.Add(3 * time.Hour)
	fmt.Println("start3h: ", start3h)

	end5h := startOfDay.Add(5 * time.Hour)
	fmt.Println("end5h: ", end5h)

	for _, s := range sessions {
		// Check if the session is on the specified date
		if s.StartTime.Year() == date.Year() && s.StartTime.YearDay() == date.YearDay() {
			// Calculate the session validity period between 3 and 5
			effectiveStart := s.StartTime
			effectiveEnd := s.EndTime

			if effectiveStart.Before(start3h) {
				effectiveStart = start3h
			}
			if effectiveEnd.After(end5h) {
				effectiveEnd = end5h
			}
			if effectiveStart.Before(effectiveEnd) {
				duration := effectiveEnd.Sub(effectiveStart)
				userKey := s.Name + s.GoogleID

				// Update total time for users
				if sessionTime, exists := totalTimeMap[userKey]; exists {
					sessionTime.TotalTime += duration
					totalTimeMap[userKey] = sessionTime
				} else {
					totalTimeMap[userKey] = SessionTime{
						Name:      s.Name,
						Email:     s.Email,
						GoogleID:  s.GoogleID,
						TotalTime: duration,
					}
				}
			}
		}
	}

	return totalTimeMap
}

// SendRequest sends a POST request with JSON data to the API
func SendRequest(url string, data interface{}) error {
	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Create a POST request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Setup headers
	req.Header.Set("User-Agent", "Reqable/2.21.0")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("securityCode", "12345678")

	// Send request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Check HTTP status codes
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed with status code: %d", resp.StatusCode)
	}

	return nil
}
