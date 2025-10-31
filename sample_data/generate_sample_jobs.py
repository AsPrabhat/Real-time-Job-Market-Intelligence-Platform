"""
Sample Job Data Generator for CareerRadar
Generates realistic job postings for testing the data pipeline
"""

import json
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('en_IN')

# Indian tech companies
COMPANIES = [
    "Tata Consultancy Services", "Infosys", "Wipro", "HCL Technologies",
    "Tech Mahindra", "Amazon India", "Microsoft India", "Google India",
    "Accenture India", "Capgemini India", "IBM India", "Oracle India",
    "Adobe India", "SAP Labs India", "Intel India", "Cisco India",
    "Qualcomm India", "PayPal India", "Flipkart", "Paytm",
    "Swiggy", "Zomato", "CRED", "PhonePe", "Razorpay"
]

# Target locations in India
LOCATIONS = [
    "Bangalore", "Hyderabad", "Mumbai", "Pune", "Delhi", "Noida",
    "Gurgaon", "Chennai"
]

# Job titles for software and AI/ML roles
JOB_TITLES = [
    "Software Development Engineer", "Senior Software Engineer", "Software Engineer",
    "AI/ML Engineer", "Machine Learning Engineer", "Data Engineer",
    "Senior Data Engineer", "Backend Engineer", "Full Stack Developer",
    "Python Developer", "Senior Python Developer", "MLOps Engineer",
    "AI Research Engineer", "Data Scientist", "Applied Scientist",
    "Cloud Engineer", "DevOps Engineer", "Software Development Engineer - Internship"
]

# Technical skills from your resume and preferences
SKILLS = [
    "Python", "C++", "Java", "SQL", "JavaScript", "R",
    "Apache Spark", "Kafka", "Delta Lake", "PostgreSQL", "MongoDB",
    "Azure", "AWS", "Docker", "Kubernetes", "Git", "GitHub",
    "Machine Learning", "Deep Learning", "NLP", "Computer Vision",
    "TensorFlow", "PyTorch", "scikit-learn", "Pandas", "NumPy",
    "LLM", "Generative AI", "RAG", "Vector Databases", "LangChain",
    "Prompt Engineering", "OpenAI", "Hugging Face",
    "Distributed Systems", "Microservices", "REST API", "GraphQL",
    "Data Pipeline", "ETL", "Data Engineering", "Big Data",
    "Node.js", "React", "Flask", "FastAPI", "Django"
]

# Experience levels
EXPERIENCE_LEVELS = {
    "Internship": ["0-6 months", "Internship"],
    "Entry-Level": ["0-2 years", "1-3 years", "Fresher"],
    "Mid-Level": ["2-5 years", "3-5 years", "4-6 years"],
    "Senior": ["5+ years", "6-10 years", "8+ years"]
}

def generate_job_description(title, skills_required):
    """Generate a realistic job description"""
    
    templates = [
        f"We are looking for a talented {title} to join our team. The ideal candidate will have strong experience with {', '.join(skills_required[:3])} and a passion for building scalable solutions.",
        
        f"Join our team as a {title}! You'll work on cutting-edge projects involving {', '.join(skills_required[:2])}. Strong problem-solving skills and experience with {skills_required[2]} are essential.",
        
        f"Exciting opportunity for a {title}! We need someone with expertise in {', '.join(skills_required[:3])} to help build next-generation applications. Experience with cloud platforms and distributed systems is a plus.",
        
        f"We're hiring a {title} to work on innovative projects. Key skills include {', '.join(skills_required[:3])}. You'll collaborate with cross-functional teams to deliver high-quality solutions.",
        
        f"As a {title}, you will design and develop robust systems using {', '.join(skills_required[:2])}. Familiarity with {skills_required[2]} and modern development practices is required."
    ]
    
    return random.choice(templates)

def determine_experience_level(title):
    """Determine experience level based on job title"""
    title_lower = title.lower()
    
    if "internship" in title_lower or "intern" in title_lower:
        return "Internship"
    elif "senior" in title_lower or "lead" in title_lower or "principal" in title_lower:
        return "Senior"
    elif "junior" in title_lower or title_lower.endswith("engineer i"):
        return "Entry-Level"
    else:
        return random.choice(["Entry-Level", "Mid-Level"])

def generate_job_posting():
    """Generate a single job posting"""
    
    company = random.choice(COMPANIES)
    title = random.choice(JOB_TITLES)
    location = random.choice(LOCATIONS)
    experience_level = determine_experience_level(title)
    experience = random.choice(EXPERIENCE_LEVELS[experience_level])
    
    # Select 3-8 random skills, ensuring variety
    num_skills = random.randint(3, 8)
    skills_required = random.sample(SKILLS, num_skills)
    
    # Generate posting date within last 30 days
    days_ago = random.randint(0, 30)
    posted_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    
    # Generate description
    description = generate_job_description(title, skills_required)
    
    # Create job ID
    job_id = f"JOB-{random.randint(10000, 99999)}"
    
    return {
        "job_id": job_id,
        "title": title,
        "company": company,
        "location": location,
        "experience": experience,
        "experience_level": experience_level,
        "skills_required": skills_required,
        "description": description,
        "posted_date": posted_date,
        "link": f"https://{company.lower().replace(' ', '')}.com/careers/{job_id}",
        "salary_range": f"{random.randint(6, 40)}-{random.randint(10, 60)} LPA" if random.random() > 0.3 else None
    }

def generate_sample_data(num_jobs=50):
    """Generate multiple job postings"""
    
    print(f"Generating {num_jobs} sample job postings...")
    
    jobs = []
    for i in range(num_jobs):
        job = generate_job_posting()
        jobs.append(job)
        
        if (i + 1) % 10 == 0:
            print(f"Generated {i + 1} jobs...")
    
    return jobs

def save_to_json(jobs, filename="sample_jobs.json"):
    """Save job postings to JSON file"""
    
    output_path = filename
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    print(f"\nSuccessfully saved {len(jobs)} job postings to {output_path}")
    print("\nSample statistics:")
    print(f"  Companies: {len(set(job['company'] for job in jobs))}")
    print(f"  Locations: {len(set(job['location'] for job in jobs))}")
    print(f"  Unique skills: {len(set(skill for job in jobs for skill in job['skills_required']))}")
    
    # Experience level distribution
    exp_levels = {}
    for job in jobs:
        level = job['experience_level']
        exp_levels[level] = exp_levels.get(level, 0) + 1
    
    print("\n  Experience level distribution:")
    for level, count in sorted(exp_levels.items()):
        print(f"    {level}: {count}")

if __name__ == "__main__":
    # Generate 50 sample job postings
    jobs = generate_sample_data(num_jobs=50)
    
    # Save to JSON file
    save_to_json(jobs)
    
    print("\n✓ Sample data generation complete!")
