from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from bson.json_util import dumps  # handles ObjectId serialization
from dotenv import load_dotenv
import os
from apikeyManager import APIKeyManager


load_dotenv() 
app = Flask(__name__)
CORS(app)

# MongoDB connection
client = MongoClient("mongodb+srv://root:root@cluster0.jt307.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client['school']
collection = db['records']
keysList = collection.find_one({'email':'apikeys'})['apikeys']
keysList = list(map(tuple, keysList))
manager = APIKeyManager(keysList)
# print(keysList)
# from assignmentfetching import student_assignment_status
# Optional: Route to insert activityLog
@app.route('/insertActivityLog', methods=['POST', 'GET'])
def insert_activity_log():
    activity_log = [
        { "date": "2025-05-20", "module": "Vocabulary", 'activity': "Learned 5 new words", 'score': 85 },
        { "date": "2025-05-20", "module": "Grammar", 'activity': "Completed passive voice exercise", 'score': 78 },
        { "date": "2025-05-19", "module": "Speaking", 'activity': "Practiced introductions", 'score': 82 },
        { "date": "2025-05-19", "module": "Story", 'activity': "Created a short story", 'score': 90 },
        { "date": "2025-05-18", "module": "Pronunciation", 'activity': "Practiced vowel sounds", 'score': 75 },
        { "date": "2025-05-17", "module": "Reflex", 'activity': "Completed basic challenge", 'score': 65 },
        { "date": "2025-05-17", "module": "Grammar", 'activity': "Practiced using articles", 'score': 88 },
        { "date": "2025-05-16", "module": "Vocabulary", 'activity': "Reviewed 10 words", 'score': 92 }
    ]

    result = collection.update_one(
        { "_id": "yashwanth71208@gmail.com" },
        { "$set": { "activityLog": activity_log } },
        upsert=True
    )
    print('Activity log inserted/updated:', result)
    return jsonify({'status': 'Activity log inserted/updated'})

@app.route('/', methods=['GET'])
def hone():
    return jsonify({"message": "Welcome to the Speakmate API!"})

# Login Route
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('email')
    password = data.get('password')

    user = collection.find_one({'email': username, 'password': password})
    
    print('Login attempt by:', username)
    if user:
        return jsonify({'success': True, 'message': 'Login successful', 'id': user['id'], 'email': user['email'], 'fullName': user['fullName'], 'role': user['role'], 'classes': user['classes'], 'sections': user['sections']})
    else:
        return jsonify({'success': False, 'message': 'Invalid username or password'})

# Get user data
@app.route('/getUserData', methods=['POST'])
def get_user_data():
    data = request.get_json()
    username = data.get('email')
    user_data = collection.find_one({'email': username})
    print('Fetching user data for:', username)
    
    if user_data:
        return dumps(user_data) 
    else:
        return jsonify({'error': 'User not found'}), 404
    
@app.route("/updatehints", methods=["POST"])
def update_hints():
    data = request.json
    email = data.get("email")
    difficulty = data.get("difficulty")
    word = data.get("word")

    if not all([email, difficulty, word]):
        return jsonify({"error": "Missing required fields"}), 400

    field_path = f"wordscramble.{difficulty}.$[entry].1"

    result = collection.update_one(
        { "email": email },
        { "$inc": { field_path: 1 } },
        array_filters=[ { "entry.0": word } ]
    )
    # print(result)

    return jsonify({
        "matched": result.matched_count,
        "modified": result.modified_count
    })


@app.route('/get-api-key', methods=['GET'])
def get_api_key():
    key, model = manager.get_available_key()
    # print(data)
    return jsonify({'key':key, 'model':model})

@app.route("/increment-score", methods=["POST"])
def mark_solved_and_update_score():
    data = request.json
    email = data.get("email")
    difficulty = data.get("difficulty") 
    word = data.get("word")

    if not all([email, difficulty, word]):
        return jsonify({ "error": "Missing fields" }), 400

    is_path = f"wordscramble.{difficulty}.$[entry].2"
    score_path = f"wordscramble.{difficulty}score.score"

    # Update: set is = true and increment score
    result = collection.update_one(
        { "email": email },
        {
            "$set": { is_path: True },
            "$inc": { score_path: 1 }
        },
        array_filters=[{ "entry.0": word, "entry.2": False }]  # only if not already solved
    )

    if result.modified_count == 0:
        return jsonify({ "message": "Already solved or word not found" })

    return jsonify({
        "message": "Word marked as solved and score incremented",
        "matched": result.matched_count,
        "modified": result.modified_count
    })

@app.route("/updateVocabularyArchadeScore", methods=["POST"])
def update_vocabulary_archade_score():
    data = request.json
    email = data["email"]
    difficulty = data["difficulty"]
    word = data["word"]

    result = collection.update_one(
        {
            "email": email,
            f"vocabularyArchade.{difficulty}.wordDetails.word": word
        },
        {
            "$set": {
                f"vocabularyArchade.{difficulty}.wordDetails.$.isSolved": True
            },
            "$inc": {
                f"vocabularyArchade.{difficulty}.score": 1
            }
        }
    )

    if result.modified_count > 0:
        return jsonify({ "success": True })
    else:
        return jsonify({ "success": False, "message": "Word not found or already solved" }), 400


@app.route("/updateVocabularyBadge", methods=["POST"])
def update_vocabulary_badge():
    data = request.json
    email = data.get("email")
    badge = data.get("badge")
    level = data.get("level")

    if not all([email, badge, level]):
        return jsonify({ "success": False, "message": "Missing required fields" }), 400

    result = collection.update_one(
        { "email": email },
        { "$set": { f"vocabularyArchade.{level}.badge": badge } }
    )

    if result.modified_count > 0:
        return jsonify({ "success": True })
    else:
        return jsonify({ "success": False, "message": "User not found or badge not updated" }), 400

@app.route('/updateWordsearchScore', methods=['POST'])
def update_wordsearch_score():
    data = request.json
    email = data.get('email')
    level = data.get('level')
    score = data.get('score')
    word = data.get('word')

    if not all([email, level, word, score is not None]):
        return jsonify({"message": "Missing fields"}), 400

    # Set score and mark word as solved
    result = collection.update_one(
        { "email": email, f"wordsearch.{level}.words.word": word.upper() },
        {
            "$set": {
                f"wordsearch.{level}.score": score,
                f"wordsearch.{level}.words.$.solved": True
            }
        }
    )
    print(result)
    if result.modified_count > 0:
        return jsonify({"message": "Score and word updated successfully"})
    else:
        return jsonify({"message": "No update made — word may not exist"}), 404
    
    
@app.route('/updateDailyData', methods=['POST'])
def update_daily_data():
    data = request.get_json()
    username = data.get('username')
    completeData = data.get('data')
    print(completeData)
    dailyData = completeData['dailyData']
    currdayobj = data.get('currDayObj')
    print('Updating daily data for:', completeData)
    print('Daily data:', dailyData)
    # return jsonify({'status': 'Daily data update endpoint reached'})

    if not username or not dailyData:
        return jsonify({'error': 'Username and dailyData are required'}), 400

    result = collection.update_one(
        { "email": username },
        { "$set": { "dailyData": dailyData } },
        upsert=True
    )
    print('Update result:', result)
    
    FIELD_MAP = {
    "speaking": "speakingCompletion",
    "pronunciation": "pronunciationCompletion",
    "grammar": "grammarCompletion",
    "vocabulary": "vocabularyCompletion",
    "reflex": "reflexCompletion",
    "story": "storyCompletion"
}
    
    if not username:
        return jsonify({"error": "Email is required"}), 400

    update_fields = {}
    print(currdayobj)
    for req_field, db_field in FIELD_MAP.items():
        if req_field in currdayobj:
            # Increment field
            update_fields[db_field] = (currdayobj[req_field] +completeData[db_field])// 2  # Average the current and new value
            

    if not update_fields:
        return jsonify({"error": "No valid score fields to update"}), 400
    print(update_fields)
    # Increment the fields in MongoDB
    result = collection.update_one(
        {"email": username},
        {"$set": update_fields}
    )

    
    if result.modified_count > 0 or result.upserted_id:
        return jsonify({'status': 'Daily data updated successfully'})
    else:
        return jsonify({'status': 'Failed to update daily data'}), 500
    # return jsonify({'status': 'Daily data updated successfully'})


@app.route('/students', methods=['GET'])
def get_students():
    class_arg = request.args.get('class')
    section_arg = request.args.get('section')
    print(class_arg, section_arg)

    if not class_arg or not section_arg:
        return jsonify({"error": "Please provide class and section as query parameters"}), 400

    pipeline = [
        {
            "$match": {
                "role": "student",
                "classes": class_arg,
                "sections": section_arg
            }
        },
        {
            "$project": {
                "_id": 0,
                "id": "$id",
                "username": {"$arrayElemAt": [{"$split": ["$email", "@"]}, 0]},
                "fullName": "$fullName",
                "class": {"$arrayElemAt": ["$classes", 0]},
                "section": {"$arrayElemAt": ["$sections", 0]},
                "speaking": "$speakingCompletion",
                "pronunciation": "$pronunciationCompletion",
                "vocabulary": "$vocabularyCompletion",
                "grammar": "$grammarCompletion",
                "story": "$storyCompletion",
                "reflex": "$reflexCompletion",
                "timeSpent": "$timeSpent",
                "overall": "$overall"
            }
        }
    ]

    results = list(collection.aggregate(pipeline))
    return jsonify(results)


@app.route('/update-wordscramble-words', methods=['POST'])
def update_words():
    data = request.json
    class_name = data.get('classes')
    section = data.get('section')
    words = data.get('words', [])

    if not class_name or not section:
        return jsonify({"message": "Missing classes or section"}), 400

    # Prepare words by difficulty
    difficulty_map = {"easy": [], "medium": [], "hard": []}
    for item in words:
        word = item.get("word")
        difficulty = item.get("difficulty")
        if difficulty in difficulty_map:
            difficulty_map[difficulty].append([word, 0, False])

    # Build update object
    update_obj = {}
    for diff, word_list in difficulty_map.items():
        if word_list:
            update_obj[f"wordscramble.{diff}"] = { "$each": word_list }

    if not update_obj:
        return jsonify({"message": "No valid words to add"}), 400

    # Perform update for all matching users
    result = collection.update_many(
        {
            "classes": class_name,
            "sections": section
        },
        {
            "$push": update_obj
        }
    )

    if result.modified_count:
        return jsonify({"message": f"Updated {result.modified_count} students successfully"})
    else:
        return jsonify({"message": "No matching students found or no updates performed"}), 404


DIFFICULTY_MAP = {
    "easy": "beginner",
    "medium": "intermediate",
    "hard": "advanced"
}

@app.route('/update-vocab', methods=['POST'])
def update_vocab():
    data = request.json
    class_name = data.get('classes')
    section = data.get('section')
    words = data.get('words', [])
    print(words)

    if not class_name or not section:
        return jsonify({"message": "Missing classes or section"}), 400

    # Build update instructions
    push_updates = {}
    for item in words:
        difficulty = item.get("difficulty")
        level = DIFFICULTY_MAP.get(difficulty).strip()

        if not level:
            continue  # skip invalid difficulty

        word_obj = {
            "word": item.get("word"),
            "definition": item.get("definition"),
            "incorrectDefinitions": item.get("wrongDefinitions", []),
            "partOfSpeech": item.get("partOfSpeech"),
            "example": item.get("example"),
            "hint": item.get("hint"),
            "isSolved": False
        }

        field_path = f"vocabularyArchade.{level}.wordDetails"
        if field_path not in push_updates:
            push_updates[field_path] = {"$each": []}
        push_updates[field_path]["$each"].append(word_obj)

    if not push_updates:
        return jsonify({"message": "No valid words to add"}), 400

    # Update all matching students
    result = collection.update_many(
        {
            "classes": class_name,
            "sections": section
        },
        {
            "$push": push_updates
        }
    )

    if result.modified_count:
        return jsonify({"message": f"Updated {result.modified_count} students successfully"})
    else:
        return jsonify({"message": "No matching students found or no updates performed"}), 404

@app.route('/update-wordsearch', methods=['POST'])
def update_wordsearch():
    data = request.json
    class_name = data.get('classes')
    section = data.get('section')
    words = data.get('words', [])

    if not class_name or not section:
        return jsonify({"message": "Missing classes or section"}), 400

    if not words:
        return jsonify({"message": "No words to add"}), 400

    # Build updates per level
    push_updates = {}

    for item in words:
        difficulty = item.get('difficulty')
        level = DIFFICULTY_MAP.get(difficulty)

        if not level:
            continue  # skip if difficulty invalid

        word_doc = {
            "word": item.get('word'),
            "hint": item.get('definition'),
            "solved": False
        }

        field_path = f"wordsearch.{level}.words"
        if field_path not in push_updates:
            push_updates[field_path] = {"$each": []}
        push_updates[field_path]["$each"].append(word_doc)

    if not push_updates:
        return jsonify({"message": "No valid words to add"}), 400

    # Update all matching documents
    result = collection.update_many(
        {
            "classes": class_name,
            "sections": section
        },
        {
            "$push": push_updates
        }
    )

    if result.modified_count:
        return jsonify({"message": f"Updated {result.modified_count} students successfully"})
    else:
        return jsonify({"message": "No matching students found or no updates performed"}), 404


def reset_int_bool(value):
    if isinstance(value, bool):
        return False
    elif isinstance(value, int):
        return 0
    elif isinstance(value, list):
        return [reset_int_bool(item) for item in value]
    elif isinstance(value, dict):
        return {k: reset_int_bool(v) for k, v in value.items()}
    else:
        return value
def create_new_document(template, new_email, new_classes, new_sections, new_password, new_fullName, new_role):
    new_doc = {}
    for key, value in template.items():
        if key == '_id':
            continue
        elif key == 'email':
            new_doc['email'] = new_email
        elif key == 'classes':
            new_doc['classes'] = new_classes
        elif key == 'sections':
            new_doc['sections'] = new_sections
        elif key == 'password':
            new_doc['password'] = new_password
        elif key == 'fullName':
            new_doc['fullName'] = new_fullName
        elif key == 'role':
            new_doc['role'] = new_role
        else:
            new_doc[key] = reset_int_bool(value)
    return new_doc
@app.route('/create_account', methods=['POST'])
def create_account():
    data = request.get_json()
    email = data.get('email')
    classes = data.get('classes', [])
    sections = data.get('section', [])
    password = data.get('password')
    fullName = data.get('fullName')
    role = data.get('role')
    print(classes, sections, password)

    if not email or not classes or not sections:
        return jsonify({'status': 'error', 'message': 'Missing fields'}), 400

    if collection.find_one({"email": email}):
        return jsonify({'status': 'exists', 'message': 'Account already exists'}), 200

    template = collection.find_one({"email": "template"})
    if not template:
        return jsonify({'status': 'error', 'message': 'Template student not found'}), 500

    new_doc = create_new_document(template, email, classes, sections, password, fullName, role)
    collection.insert_one(new_doc)
    # print(new_doc)

    return jsonify({'status': 'success', 'message': 'Account created'}), 201


@app.route('/get-assignments', methods=['POST'])
def get_assignments():
    data = request.get_json()

    email = data.get('email')
    # teacher_classes = data.get('teacherClass')
    # sections = data.get('section')

    if not email :
        return jsonify({"error": "Missing email, teacherClass, or section"}), 400

    # Fetch teacher document
    teacher_doc = collection.find_one({"email": email})
    
    if not teacher_doc:
        return jsonify({"error": "Teacher not found"}), 404

    assignments = teacher_doc.get('assignments', [])
    
    # # Filter assignments by matching class and section
    # filtered_assignments = [
    #     a for a in assignments
    #     if a.get('targetClass') in teacher_classes and a.get('targetSection') in sections
    # ]
    print(assignments)
    return jsonify({"assignments": assignments}), 200

@app.route('/add-assignment', methods=['POST'])
def add_assignment():
    data = request.get_json()
    email = data.get('email')
    assignment = data.get('newAssignment')

    if not email or not assignment:
        return jsonify({"error": "Missing email or assignment data"}), 400

    # Find teacher document
    teacher_doc = collection.find_one({"email": email})

    if teacher_doc:
        # If assignments field exists, append
        if 'assignments' in teacher_doc:
            collection.update_one(
                {"email": email},
                {"$push": {"assignments": assignment}}
            )
        else:
            # Create new assignments array
            collection.update_one(
                {"email": email},
                {"$set": {"assignments": [assignment]}}
            )
        return jsonify({"message": "Assignment added successfully."})
    else:
        return jsonify({"error": "Teacher not found"}), 404


@app.route("/delete-assignment", methods=["POST"])
def delete_assignment():
    data = request.get_json()
    email = data.get("email")
    assignment_id = data.get("id")

    if not email or not assignment_id:
        return jsonify({"error": "Missing email or assignment_id"}), 400

    teacher = collection.find_one({"email": email})
    if not teacher:
        return jsonify({"error": "Teacher not found"}), 404

    assignment = next((a for a in teacher.get("assignments", []) if a.get("id") == assignment_id), None)
    if not assignment:
        return jsonify({"error": "Assignment not found"}), 404

    assignment_type = assignment["type"]
    target_class = assignment["targetClass"]
    target_section = assignment["targetSection"]
    metadata = assignment.get("metadata", {})

    word = None
    difficulty = None
    modified_count = 0

    if assignment_type == "word_scramble":
        word_info = metadata.get("scrambleWords", [{}])[0]
        word = word_info.get("word")
        difficulty = word_info.get("difficulty")

        users = collection.find({
            "classes": target_class,
            "sections": target_section
        })

        for user in users:
            scramble_list = user.get("wordscramble", {}).get(difficulty, [])
            matched_item = next((item for item in scramble_list if item[0] == word), None)

            if matched_item:
                # Check if word was solved
                was_solved = matched_item[2] == True

                # Remove the word entry
                collection.update_one(
                    {"_id": user["_id"]},
                    {"$pull": {f"wordscramble.{difficulty}": matched_item}}
                )

                # If solved, decrement score
                if was_solved:
                    collection.update_one(
                        {"_id": user["_id"]},
                        {"$inc": {f"wordscramble.{difficulty}score.score": -1}}
                    )
                modified_count += 1

    elif assignment_type == "word_search":
        word_info = metadata.get("searchWords", [{}])[0]
        word = word_info.get("word")
        difficulty = word_info.get("difficulty")
        difficulty = DIFFICULTY_MAP[difficulty]

        users = collection.find({
            "classes": target_class,
            "sections": target_section
        })

        for user in users:
            words = user.get("wordsearch", {}).get(difficulty, {}).get("words", [])
            matched_word = next((w for w in words if w["word"] == word), None)

            if matched_word:
                was_solved = matched_word.get("solved", False)

                # Remove the word
                collection.update_one(
                    {"_id": user["_id"]},
                    {"$pull": {f"wordsearch.{difficulty}.words": {"word": word}}}
                )

                # Decrement score if needed
                if was_solved:
                    collection.update_one(
                        {"_id": user["_id"]},
                        {"$inc": {f"wordsearch.{difficulty}.score": -1}}
                    )
                modified_count += 1

    elif assignment_type == "vocabulary_builder":
        word_info = metadata.get("vocabularyWords", [{}])[0]
        word = word_info.get("word")
        difficulty = word_info.get("difficulty")
        difficulty = DIFFICULTY_MAP[difficulty]

        users = collection.find({
            "classes": target_class,
            "sections": target_section
        })

        for user in users:
            details = user.get("vocabularyArchade", {}).get(difficulty, {}).get("wordDetails", [])
            matched_word = next((w for w in details if w["word"] == word), None)

            if matched_word:
                was_solved = matched_word.get("isSolved", False)

                # Remove the word
                collection.update_one(
                    {"_id": user["_id"]},
                    {"$pull": {f"vocabularyArchade.{difficulty}.wordDetails": {"word": word}}}
                )

                # Decrement score if solved
                if was_solved:
                    collection.update_one(
                        {"_id": user["_id"]},
                        {"$inc": {f"vocabularyArchade.{difficulty}.score": -1}}
                    )
                modified_count += 1

    else:
        return jsonify({"error": "Unknown assignment type"}), 400

    # Remove assignment from teacher
    collection.update_one(
        {"email": email},
        {"$pull": {"assignments": {"id": assignment_id}}}
    )

    return jsonify({
        "success": True,
        "assignmentDeleted": True,
        "wordDeleted": word,
        "usersModified": modified_count
    }), 200



@app.route("/student-overall-progress", methods=["POST"])
def student_overall_progress():
    data = request.json
    student_email = data.get("studentEmail")
    print(student_email, data)
    if not student_email:
        return jsonify({"error": "Missing studentEmail"}), 400

    # 1. Fetch student document
    student = collection.find_one({"email": student_email, "role": "student"})
    if not student:
        return jsonify({"error": "Student not found"}), 404

    total_items = 0
    completed_items = 0

    # 2. Check wordscramble progress
    for difficulty in ["easy", "medium", "hard"]:
        scramble_list = student.get("wordscramble", {}).get(difficulty, [])
        for item in scramble_list:
            total_items += 1
            if item[2]:  # isCompleted flag
                completed_items += 1

    # 3. Check vocabularyArcade progress
    arcade_levels = ["beginner", "intermediate", "advanced"]
    for level in arcade_levels:
        word_details = student.get("vocabularyArchade", {}).get(level, {}).get("wordDetails", [])
        for word in word_details:
            total_items += 1
            if word.get("isSolved"):
                completed_items += 1

    # 4. Check wordsearch progress
    for level in ["beginner", "intermediate", "advanced"]:
        words = student.get("wordsearch", {}).get(level, {}).get("words", [])
        for word in words:
            total_items += 1
            if word.get("solved"):
                completed_items += 1

    # 5. Calculate percentage
    percentage = (completed_items / total_items * 100) if total_items else 0

    return jsonify({
        "studentEmail": student_email,
        "totalItems": total_items,
        "completedItems": completed_items,
        "percentage": round(percentage, 2)
    })



@app.route("/student-assignment-status", methods=["POST"])
def student_assignment_status():
    data = request.json
    student_email = data.get("studentEmail")
    assignment_id = data.get("assignmentId")

    if not student_email or not assignment_id:
        return jsonify({"error": "Missing studentEmail or assignmentId"}), 400

    # 1. Find student
    student = collection.find_one({"email": student_email, "role": "student"})
    if not student:
        return jsonify({"error": "Student not found"}), 404

    # 2. Find teacher who owns the assignment
    teacher = collection.find_one({
        "role": "teacher",
        "assignments.id": assignment_id
    })
    if not teacher:
        return jsonify({"error": "Assignment not found"}), 404

    # 3. Extract assignment
    assignment = next(
        a for a in teacher["assignments"]
        if a["id"] == assignment_id
    )
    assignment_type = assignment["type"]
    metadata = assignment["metadata"]
    target_class = assignment["targetClass"]
    target_section = assignment["targetSection"]

    # 4. Check student class/section match
    if target_class not in student.get("classes", []) or target_section not in student.get("sections", []):
        return jsonify({"error": "Student not in target class/section"}), 400

    # 5. Check progress
    total_items = 0
    completed_items = 0

    if assignment_type == "word_search":
        words = metadata.get("searchWords", [])
        for w in words:
            difficulty = w["difficulty"]
            student_words = student.get("wordsearch", {}).get(difficulty, {}).get("words", [])
            match = next((sw for sw in student_words if sw["word"] == w["word"]), None)
            if match and match.get("solved"):
                completed_items += 1
            total_items += 1

    elif assignment_type == "vocabulary_builder":
        words = metadata.get("vocabularyWords", [])
        for w in words:
            difficulty = w["difficulty"]
            arcade_level = {"easy": "beginner", "medium": "intermediate", "hard": "advanced"}[difficulty]
            student_words = student.get("vocabularyArchade", {}).get(arcade_level, {}).get("wordDetails", [])
            match = next((sw for sw in student_words if sw["word"] == w["word"]), None)
            if match and match.get("isSolved"):
                completed_items += 1
            total_items += 1

    elif assignment_type == "word_scramble":
        words = metadata.get("scrambleWords", [])
        for w in words:
            difficulty = w["difficulty"]
            student_list = student.get("wordscramble", {}).get(difficulty, [])
            match = next((item for item in student_list if item[0] == w["word"]), None)
            if match and match[2]:  # third index is completion flag
                completed_items += 1
            total_items += 1

    # 6. Calculate percentage
    percentage = (completed_items / total_items * 100) if total_items else 0

    return jsonify({
        "assignmentId": assignment_id,
        "studentEmail": student_email,
        "totalItems": total_items,
        "completedItems": completed_items,
        "percentage": round(percentage, 2)
    })



@app.route("/teacher-assignments-progress", methods=["POST"])
def teacher_assignments_progress():
    data = request.json
    teacher_email = data.get("teacherEmail")

    if not teacher_email:
        return jsonify({"error": "Missing teacherEmail"}), 400

    # 1. Find teacher
    teacher = collection.find_one({"email": teacher_email, "role": "teacher"})
    if not teacher:
        return jsonify({"error": "Teacher not found"}), 404

    assignments = teacher.get("assignments", [])
    results = []

    for assignment in assignments:
        assignment_id = assignment.get("id")
        assignment_type = assignment.get("type")
        metadata = assignment.get("metadata")
        target_class = assignment.get("targetClass")
        target_section = assignment.get("targetSection")

        # 2. Find students in the target class and section
        students = list(collection.find({
            "role": "student",
            "classes": target_class,
            "sections": target_section
        }))

        for student in students:
            student_email = student.get("email")

            total_items = 0
            completed_items = 0

            if assignment_type == "word_search":
                words = metadata.get("searchWords", [])
                total_items = len(words)
                for w in words:
                    difficulty = w["difficulty"]
                    student_words = student.get("wordsearch", {}).get(difficulty, {}).get("words", [])
                    match = next((sw for sw in student_words if sw["word"] == w["word"]), None)
                    if match and match.get("solved"):
                        completed_items += 1

            elif assignment_type == "vocabulary_builder":
                words = metadata.get("vocabularyWords", [])
                total_items = len(words)
                for w in words:
                    difficulty = w["difficulty"]
                    arcade_level = {"easy": "beginner", "medium": "intermediate", "hard": "advanced"}[difficulty]
                    student_words = student.get("vocabularyArchade", {}).get(arcade_level, {}).get("wordDetails", [])
                    match = next((sw for sw in student_words if sw["word"] == w["word"]), None)
                    if match and match.get("isSolved"):
                        completed_items += 1

            elif assignment_type == "word_scramble":
                words = metadata.get("scrambleWords", [])
                total_items = len(words)
                for w in words:
                    difficulty = w["difficulty"]
                    student_list = student.get("wordscramble", {}).get(difficulty, [])
                    match = next((item for item in student_list if item[0] == w["word"]), None)
                    if match and match[2]:
                        completed_items += 1

            # 3. Compute bestScore
            if total_items > 0:
                best_score = round((completed_items / total_items) * 100, 2)
            else:
                best_score = 0.0

            # 4. Determine status
            status = "completed" if best_score == 100.0 else "incomplete"

            # 5. Add to results
            results.append({
                "assignmentId": assignment_id,
                "studentId": student_email,
                "attempts": 0,
                "bestScore": best_score,
                "timeSpent": 0,
                "status": status,
                "lastAttempt": None
            })

    return jsonify(results)
if __name__ == '__main__':
    app.run(debug=True)
