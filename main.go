package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
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
	GoogleID  string    `json:"google_id"`
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`
}

type SessionTime struct {
	Name      string        `json:"fullName"`
	GoogleID  string        `json:"googleId"`
	TotalTime time.Duration `json:"totalTime"`
	Date      time.Time     `json:"date"`
}

// Custom JSON marshaling
func (s SessionTime) MarshalJSON() ([]byte, error) {
	type Alias SessionTime
	totalMinutes := int(math.Round(s.TotalTime.Minutes()))
	return json.Marshal(&struct {
		Name      string `json:"fullName"`
		GoogleID  string `json:"googleId"`
		TotalTime int    `json:"totalTime"`
		Date      string `json:"date"`
		*Alias
	}{
		Name:     s.Name,
		GoogleID: s.GoogleID,
		// Convert `TotalTime` to string in the format "hh:mm:ss"
		TotalTime: totalMinutes,
		// Format `Date` as a string in the format "yyyy-mm-dd" (adjust as needed)
		Date:  s.Date.Format("2006-01-02"),
		Alias: (*Alias)(&s),
	})
}

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	// Get environment variables from .env file
	host := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	serverPort := os.Getenv("SERVER_PORT")
	user := os.Getenv("DB_USERNAME")
	dbname := os.Getenv("DB_DATABASE")
	password := os.Getenv("DB_PASSWORD")
	sslmode := os.Getenv("DB_SSLMODE")
	tableName := os.Getenv("VOICE_CHANNEL_USER_TABLE")
	apiPath := os.Getenv("API_PATH")
	securityCode := os.Getenv("SECURITYCODE")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s", user, password, dbname, host, dbPort, sslmode)

	// Create handler for API with totalTimeMap
	http.HandleFunc(apiPath, func(w http.ResponseWriter, r *http.Request) {
		// Get time parameter from query string
		timeParam := r.URL.Query().Get("time")
		clanID := r.URL.Query().Get("clanID")

		// Default to 6 days ago
		now := time.Now()
		utcNow := now.UTC()
		twoDaysAgo := utcNow.AddDate(0, 0, -6)
		date := twoDaysAgo

		if timeParam != "" {
			// Try parsing the custom format yyyy/mm/dd
			parsedTime, err := parseCustomDateFormat(timeParam)
			if err != nil {
				log.Printf("Invalid time parameter (custom format): %v, using default time", err)
			} else {
				date = parsedTime
			}
		}

		// Fetch activities and process them
		activities, err := FetchActivities(connStr, tableName, date, clanID)
		if err != nil {
			log.Fatal(err)
		}

		// Sort by name and creation time
		SortActivities(activities)

		// Handle user sessions
		sessions := processActivities(activities)

		// Filters sessions that reside entirely within other sessions
		filteredSessions := FilterSessions(sessions)

		// Calculate the total time of each opentalk participant during the day
		totalTime := CalculateTotalTimeForDate(filteredSessions, date)

		totalTimeMap := mapToSlice(totalTime)

		// Create handler for API with totalTimeMap
		createHandleSessions(totalTimeMap, securityCode)(w, r)
	})

	// Launch the server and report errors if any
	serverPort1 := ":" + serverPort
	log.Printf("Starting server on port %s...", serverPort1)
	if err := http.ListenAndServe(serverPort1, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// Convert yyyy/mm/dd to time.Time
func parseCustomDateFormat(dateStr string) (time.Time, error) {
	// Parse yyyy/mm/dd format
	parsedDate, err := time.Parse("2006/01/02", dateStr)
	if err != nil {
		return time.Time{}, err
	}

	// Convert to RFC3339 format (yyyy-mm-ddThh:mm:ssZ)
	// Set time to the beginning of the day in UTC
	return time.Date(parsedDate.Year(), parsedDate.Month(), parsedDate.Day(), 0, 0, 0, 0, time.UTC), nil
}

func mapToSlice(m map[string]SessionTime) []SessionTime {
	var slice []SessionTime
	for _, v := range m {
		slice = append(slice, v)
	}
	return slice
}

// Get data from database
func FetchActivities(connStr string, tableName string, date time.Time, clandID string) ([]VoiceChannelUser, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Calculate the start and end of the day in UTC
	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	endOfDay := startOfDay.Add(24 * time.Hour).Add(-time.Second)

	startOfDayStr := startOfDay.Format(time.RFC3339)
	endOfDayStr := endOfDay.Format(time.RFC3339)

	query := fmt.Sprintf("SELECT * FROM %s WHERE create_time BETWEEN $1 AND $2", tableName)

	var rows *sql.Rows
	if clandID == "" {
		rows, err = db.Query(query, startOfDayStr, endOfDayStr)
	} else {
		query += " AND clan_id = $3"
		rows, err = db.Query(query, startOfDayStr, endOfDayStr, clandID)
	}

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

// Calculate the total activity time of each user within a certain level in 1 day
func CalculateTotalTimeForDate(sessions []Session, date time.Time) map[string]SessionTime {
	totalTimeMap := make(map[string]SessionTime)

	// Determine the time interval from 3 to 5 UTC (10 to 12 UTC +7)
	startOfDay := date.Truncate(24 * time.Hour)
	start3h := startOfDay.Add(3 * time.Hour)
	end5h := startOfDay.Add(5 * time.Hour)

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
						GoogleID:  s.GoogleID,
						TotalTime: duration,
						Date:      startOfDay,
					}
				}
			}
		}
	}

	return totalTimeMap
}

// API handling with totalTime
func createHandleSessions(sessionTimes []SessionTime, securityCode string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		// Check Security-Code
		if r.Header.Get("Security-Code") != securityCode {
			http.Error(w, "Unauthorized Security-Code", http.StatusUnauthorized)
			return
		}

		// Settings header to return JSON
		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(sessionTimes)
	}
}
